from django.urls import path
from rest_framework.routers import DefaultRouter
from rest_framework.authtoken import views

from restapi.views import UserViewSet, CategoryViewSet, GroupViewSet, ExpenseViewSet, index, logout, balance, \
    log_processor


router = DefaultRouter()
router.register('users', UserViewSet)
router.register('categories', CategoryViewSet)
router.register('groups', GroupViewSet)
router.register('expenses', ExpenseViewSet)

urlpatterns = [
    path('', index, name='index'),
    path('auth/logout/', logout),
    path('auth/login/', views.obtain_auth_token),
    path('balances/', balance),
    path('process-logs/', log_processor)
]

urlpatterns += router.urls
