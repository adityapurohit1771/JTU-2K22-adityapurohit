# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
import restapi.models_constants as consts
# Create your models here.
from django.contrib.auth.models import User

class Category(models.Model):
    name = models.CharField(max_length=consts.CATEGORY_MAX_LENGTH, null=False)


class Groups(models.Model):
    name = models.CharField(max_length=consts.GROUPS_MAX_LENGTH, null=False)
    members = models.ManyToManyField(User, related_name='members', blank=True)

class Expenses(models.Model):
    description = models.CharField(max_length=consts.EXPENSES_MAX_LENGTH)
    total_amount = models.DecimalField(max_digits=consts.EXPENSES_MAX_DIGITS, decimal_places=consts.EXPENSES_DECIMAL_PLACES)
    group = models.ForeignKey(Groups, null=True, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, default=1, on_delete=models.CASCADE)


class UserExpense(models.Model):
    expense = models.ForeignKey(Expenses, default=1, on_delete=models.CASCADE, related_name="users")
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="expenses")
    amount_owed = models.DecimalField(max_digits=consts.USER_EXPENSE_MAX_DIGITS, decimal_places=consts.USEREXPENSE_DECIMAL_PLACES)
    amount_lent = models.DecimalField(max_digits=consts.USER_EXPENSE_MAX_DIGITS, decimal_places=consts.USEREXPENSE_DECIMAL_PLACES)

    def __str__(self):
        return f"user: {self.user}, amount_owed: {self.amount_owed} amount_lent: {self.amount_lent}"
