#!/opt/conda/bin/python

import sys
import datetime

import papermill as pm
from papermill.exceptions import PapermillExecutionError

#from send_slack_alert import send_alert


print()
print()
print('---------------------------------------------------------------------------')
local_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(local_now)

if len(sys.argv) < 2:
    print("must provide notebook path")
    exit(1)

try:
    nb_path = sys.argv[1]
    nb_filename = nb_path.split("/")[-1]
    
    pm.execute_notebook(
       nb_path,
       nb_path.replace(nb_filename, f"scheduling/output_notebooks/{nb_filename.split('.')[0]}_output.ipynb")
    )
    
except (FileNotFoundError, PapermillExecutionError) as e:
    print(e)
    #print('sending alert')
    #send_alert(nb_filename)

else:
    print()
    print('no catched alerts')