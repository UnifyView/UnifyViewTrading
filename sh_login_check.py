from Module.generic_helper import get_env
from Module.shoonya_helper import sh_get_api,sh_update_scripts
from datetime import datetime, timedelta
import pandas as pd

INDEX_NAME = 'NIFTY BANK'

# Get All Env Variables
config_file = "cred_sh_hs.yml"
config = get_env(config_file)

conf_file = "conf_bank_nifty.yml"
conf = get_env(conf_file)

ftApi = sh_get_api(config)
print(ftApi.get_limits())
sh_update_scripts(conf)


