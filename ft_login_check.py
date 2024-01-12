from Module.generic_helper import get_env
from Module.flattrade_helper import ft_get_api,ft_get_script_token,ft_update_scripts
from datetime import datetime, timedelta
import pandas as pd

INDEX_NAME = 'NIFTY BANK'

# Get All Env Variables
config_file = "cred_ft_hs.yml"
config = get_env(config_file)

conf_file = "conf_bank_nifty.yml"
conf = get_env(conf_file)

ftApi = ft_get_api(config)
print(ftApi.get_limits())
ft_update_scripts(conf)


