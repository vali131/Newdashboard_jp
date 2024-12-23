from django.shortcuts import render
from .models import PendingCountFetcher

def dashboard_view(request):
    counts = PendingCountFetcher.fetch_pending_counts()
    return render(request, 'dashboard.html', {'counts': counts})
