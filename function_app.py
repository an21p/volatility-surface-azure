import azure.functions as func 
from volatility_surface import volatility_surface
from downloader_trigger import downloader_trigger

app = func.FunctionApp() 

app.register_functions(volatility_surface) 
app.register_functions(downloader_trigger) 
