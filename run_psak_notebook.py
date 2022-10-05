#!/opt/conda/bin/python
import sys
import papermill as pm

if len(sys.argv) < 2:
    print("must provide notebook path")
    exit(1)

nb_name = sys.argv[1].split("/")[-1]

pm.execute_notebook(
   sys.argv[1],
   sys.argv[1].replace(nb_name, f"{nb_name.split('.')[0]}_output.ipynb")
)
