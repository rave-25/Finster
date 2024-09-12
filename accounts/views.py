
from django.shortcuts import render
from index import real_data, win_real_data, non_double

       
def home(request):
  lose_ticker_data = real_data['syms'].tolist() 
  lose_tp_data = real_data['tp'].tolist() 
  lose_sl_data = real_data['sl'].tolist() 
  win_ticker_data = win_real_data['syms'].tolist()
  win_tp_data = win_real_data['tp'].tolist()
  win_sl_data = win_real_data['sl'].tolist()
  non_double_ticker = non_double['syms'].tolist()
  new_size = non_double['new_size'].tolist()
  
  no_double = len(non_double)
  
  context = {
    'lose_ticker_data': lose_ticker_data,
    'lose_tp_data': lose_tp_data,
    'lose_sl_data': lose_sl_data,
    'win_ticker_data': win_ticker_data ,
    'win_tp_data': win_tp_data,
    'win_sl_data': win_sl_data,
    'non_double_ticker': non_double_ticker,
    'new_size': new_size,
    'no_double': no_double
  }
    
  return render(request, 'accounts/index.html', context)
