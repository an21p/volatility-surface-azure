import azure.functions as func 
from volatility_surface import option_data
from downloader_trigger import downloader_trigger

app = func.FunctionApp() 

app.register_functions(option_data) 
app.register_functions(downloader_trigger) 
