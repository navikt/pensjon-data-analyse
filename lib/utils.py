from google.cloud import secretmanager
import os
import random


def set_secrets_as_env(split_on=':'):
    secrets = secretmanager.SecretManagerServiceClient()
    resource_name = f"projects/knada-gcp/secrets/vebjorn-rekkebo-bac1/versions/latest"
    secret = secrets.access_secret_version(name=resource_name)
    secrets = secret.payload.data.decode('UTF-8')
    for secret in secrets.splitlines():
        key, value = secret.split(split_on)
        os.environ[key] = value


def randomize_zeros(x):
    if x == 0:
        return random.randint(0,5)
    else:
        return x
    