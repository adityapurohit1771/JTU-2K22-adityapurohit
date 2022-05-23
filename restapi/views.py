# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from http.client import HTTPException
import urllib.request
from datetime import datetime
from utils.set_logger import logger

from django.http import HttpResponse
from django.contrib.auth.models import User

# Create your views here.
from rest_framework.permissions import AllowAny
from rest_framework.decorators import api_view, action, authentication_classes, permission_classes
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from rest_framework import status

from restapi.models import Category, Groups, Expenses, UserExpense
from restapi.serializers import UserSerializer, CategorySerializer, GroupSerializer, ExpensesSerializer
from restapi.custom_exception import UnauthorizedUserException
import restapi.views_constants as consts

def index(_request):
    return HttpResponse("Hello, world. You're at Rest.")


@api_view(['POST'])
def logout(request):
    logger.info(f"Deleting auth_token for {request.user.id}")
    request.user.auth_token.delete()
    logger.info(f"auth_token deleted. for {request.user.id}")
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(['GET'])
def balance(request):
    logger.info("Getting Balances")
    user = request.user
    expenses = Expenses.objects.filter(users__in=user.expenses.all())
    final_balance = {}
    for expense in expenses:
        expense_balances = normalize(expense)
        for expense_balance in expense_balances:
            from_user = expense_balance['from_user']
            to_user = expense_balance['to_user']
            if from_user == user.id:
                final_balance[to_user] = final_balance.get(to_user, 0) - expense_balance['amount']
            if to_user == user.id:
                final_balance[from_user] = final_balance.get(from_user, 0) + expense_balance['amount']
    final_balance = {user_id: amount for user_id, amount in final_balance.items() if amount != 0}

    response = [{"user": user_id, "amount": int(amount)} for user_id, amount in final_balance.items()]
    logger.info("Balances retured for the request")
    return Response(response, status=status.HTTP_200_OK)


def normalize(expense):
    logger.info("Normalising expense")
    user_balances = expense.users.all()
    dues = []
    for user_balance in user_balances:
        dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                  - user_balance.amount_owed
    dues = list(sorted(dues.items(), key=lambda item: item[1]))
    start = 0
    end = len(dues) - 1
    balances = []
    while start < end:
        amount = min(abs(dues[start][1]), abs(dues[end][1]))
        user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": amount}
        balances.append(user_balance)
        dues[start] = (dues[start][0], dues[start][1] + amount)
        dues[end] = (dues[end][0], dues[end][1] - amount)
        if dues[start][1] == 0:
            start += 1
        else:
            end -= 1
    logger.info("Expenses normalised")
    return balances


class UserViewSet(ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (AllowAny,)


class CategoryViewSet(ModelViewSet):
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    http_method_names = ['get', 'post']


class GroupViewSet(ModelViewSet):
    queryset = Groups.objects.all()
    serializer_class = GroupSerializer

    def get_queryset(self):
        user = self.request.user
        groups = user.members.all()
        if self.request.query_params.get('q', None) is not None:
            groups = groups.filter(name__icontains=self.request.query_params.get('q', None))
        return groups

    def create(self, request, *args, **kwargs):
        logger.info("Creating New User")
        try:
            user = self.request.user
            data = self.request.data
        except:
            logger.error("Failed to fetch user credentials")
            return Response({"status": "failure", "reason": "Invalid user credentials"},
            status=status.HTTP_400_BAD_REQUEST)
        group = Groups(**data)
        group.save()
        group.members.add(user)
        serializer = self.get_serializer(group)
        logger.info("User created")
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(methods=['put'], detail=True)
    def members(self, request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            logger.error("User not authorized")
            raise UnauthorizedUserException()
        body = request.data
        if body.get('add', None) is not None and body['add'].get('user_ids', None) is not None:
            added_ids = body['add']['user_ids']
            for user_id in added_ids:
                group.members.add(user_id)
                logger.info(f"User {user_id} added.")
        if body.get('remove', None) is not None and body['remove'].get('user_ids', None) is not None:
            removed_ids = body['remove']['user_ids']
            for user_id in removed_ids:
                group.members.remove(user_id)
                logger.info(f"User {user_id} removed.")
        group.save()
        logger.info("Group updated")
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(methods=['get'], detail=True)
    def expenses(self, _request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            logger.error("User not authorized")
            raise UnauthorizedUserException()
        expenses = group.expenses_set
        serializer = ExpensesSerializer(expenses, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(methods=['get'], detail=True)
    def balances(self, _request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            logger.error("User not authorized")
            raise UnauthorizedUserException()
        expenses = Expenses.objects.filter(group=group)
        dues = {}
        logger.info("Calculating dues")
        for expense in expenses:
            user_balances = UserExpense.objects.filter(expense=expense)
            for user_balance in user_balances:
                dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                          - user_balance.amount_owed
        dues = list(sorted(dues.items(), key=lambda item: item[1]))
        logger.info("Dues Calculated")
        start = 0
        end = len(dues) - 1
        balances = []
        logger.info("Creating Balance Sheet")
        while start < end:
            amount = min(abs(dues[start][1]), abs(dues[end][1]))
            amount = Decimal(amount).quantize(Decimal(10)**-2)
            user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": str(amount)}
            balances.append(user_balance)
            dues[start] = (dues[start][0], dues[start][1] + amount)
            dues[end] = (dues[end][0], dues[end][1] - amount)
            if dues[start][1] == 0:
                start += 1
            else:
                end -= 1
        
        logger.info("Balance Sheet Created")

        return Response(balances, status=status.HTTP_200_OK)


class ExpenseViewSet(ModelViewSet):
    queryset = Expenses.objects.all()
    serializer_class = ExpensesSerializer

    def get_queryset(self):
        user = self.request.user
        if self.request.query_params.get('q', None) is not None:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())\
                .filter(description__icontains=self.request.query_params.get('q', None))
        else:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())
        return expenses

@api_view(['post'])
@authentication_classes([])
@permission_classes([])
def log_processor(request):
    logger.info("proccesing log data")
    try:
        data = request.data
        num_threads = data['parallelFileProcessingCount']
        log_files = data['logFiles']
    except:
        logger.error("Failed to fetch data for log proccesing")
        return Response({"status": "failure", "reason": "Invalid user credentials"},
        status=status.HTTP_400_BAD_REQUEST)
        
    if num_threads <= 0 or num_threads > 30:
        return Response({"status": "failure", "reason": "Parallel Processing Count out of expected bounds"},
                        status=status.HTTP_400_BAD_REQUEST)
    if len(log_files) == 0:
        return Response({"status": "failure", "reason": "No log files provided in request"},
                        status=status.HTTP_400_BAD_REQUEST)
    try:
        logs = multi_thread_reader(urls=data['logFiles'], num_threads=data['parallelFileProcessingCount'])
    except:
        logger.error("unable to fetch logs through multi_thread_reader")
        return Response({"status": "failure", "reason": "multi_thread_reader not working"},
        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    sorted_logs = sort_by_time_stamp(logs)
    cleaned = transform(sorted_logs)
    data = aggregate(cleaned)
    response = response_format(data)
    logger.info("Log proccessing done.")
    return Response({"response":response}, status=status.HTTP_200_OK)

def sort_by_time_stamp(logs:list)->list:
    data = []
    for log in logs:
        data.append(log.split(" "))
    # print(data)
    data = sorted(data, key=lambda elem: elem[1])
    return data

def response_format(raw_data):
    response = []
    for timestamp, data in raw_data.items():
        entry = {'timestamp': timestamp}
        logs = []
        data = {k: data[k] for k in sorted(data.keys())}
        for exception, count in data.items():
            logs.append({'exception': exception, 'count': count})
        entry['logs'] = logs
        response.append(entry)
    return response

def aggregate(cleaned_logs:list)->list:
    data = {}
    for log in cleaned_logs:
        [key, text] = log
        value = data.get(key, {})
        value[text] = value.get(text, 0)+1
        data[key] = value
    return data


def transform(logs:list)->list:
    result:list = []
    for log in logs:
        [_, timestamp, text] = log
        text:str = text.rstrip()
        timestamp = datetime.utcfromtimestamp(int(int(timestamp)/1000))
        hours, minutes = timestamp.hour, timestamp.minute
        key = ''

        if minutes >= 45:
            if hours == 23:
                key = f"{hours}:45-00:00"
            else:
                key = f"{hours}:45-{hours+1}:00"
        elif minutes >= 30:
            key = f"{hours}:30-{hours}:45"
        elif minutes >= 15:
            key = f"{hours}:15-{hours}:30"
        else:
            key = f"{hours}:00-{hours}:15"

        result.append([key, text])
        print(key)

    return result


def reader(url:str, timeout:int)->str:
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        data:str = conn.read()
        data = data.decode('utf-8')
        return data


def multi_thread_reader(urls:list, num_threads:int)->list:
    """
        Read multiple files through HTTP
    """
    result = []
    with ThreadPoolExecutor(max_workers=min(32, num_threads)) as executor:
        logs:list = [executor.submit(reader, url, consts.EXECUTOR_TIMEOUT) for url in urls]
    for log in as_completed(logs):
        result.extend(log.split("\n"))
    result = sorted(result, key=lambda elem:elem[1])
    return result
