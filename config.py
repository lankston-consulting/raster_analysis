#!/usr/bin/env python
import os
from dotenv import load_dotenv
from lcutils import gcs, eet

load_dotenv()

###############################################################################
#                               Initialization.                               #
###############################################################################
storage_keyfile = os.getenv("CS_SA_KEY_FILE")
json_ee_keyfile = os.getenv("EE_SA_KEY_FILE")

service_account = "earthengine-sa@fuelcast.iam.gserviceaccount.com"

if storage_keyfile is not None and os.path.exists(storage_keyfile):
    gch = gcs.GcsTools(use_service_account={"keyfile": storage_keyfile})
else:
    gch = gcs.GcsTools()

# if json_ee_keyfile is not None and os.path.exists(json_ee_keyfile):
#     eeh = eet.EeTools(
#         use_service_account={"account": service_account, "keyfile": json_ee_keyfile}
#     )
# else:
#     eeh = eet.EeTools()