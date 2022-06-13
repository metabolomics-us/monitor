import time

import simplejson as json
from cisclient.client import CISClient
from loguru import logger
from stasis_client.client import StasisClient


class Scheduler:
    def __init__(self, stasis_cli: StasisClient, cis_cli: CISClient):
        self.stasis_cli = stasis_cli
        self.cis_cli = cis_cli

    def schedule_sample(self, sample_id):
        try:
            # get sample data
            sample_data = self.stasis_cli.sample_acquisition_get(sample_id)
        except Exception as ex:
            logger.error(f"Can't schedule sample_id '{sample_id}'. Stasis client complained with: {str(ex)}")
            return None

        logger.info(json.dumps(sample_data, indent=2))

        # build method string
        method = ' | '.join([sample_data['chromatography']['method'], sample_data['chromatography']['instrument'],
                             sample_data['chromatography']['column'], sample_data['chromatography']['ionisation']])
        version = 'fixed'
        try:
            # get profile data
            profile_list = self.cis_cli.get_unique_method_profiles(method)
            if len(profile_list) > 0:
                raise Exception("")

            profiles = ','.join(profile_list.map(lambda x: x['profile']))
            version = list(set(profile_list.map(lambda x: x['version'])))[0]

        except Exception as ex:
            logger.error(f"Can't find valid profiles for method '{method}' and version '{version}'")
            return None

        job = {'id': f'preprocess_${time.strftime("YYYY-MM-dd", time.localtime())}_${sample_id}',
               'method': method,
               'profile': profiles,
               'samples': [sample_id]
               }

        try:
            stored_samples = self.stasis_cli.store_job(job)

            if stored_samples['statusCode'] == 200:
                response = self.stasis_cli.schedule_job(job['id'])
                logger.info(f"Sample ${sample_id} successfully scheduled.")
                return response
            else:
                logger.error(f"Error scheduling job ${job['id']}")
                return None

        except Exception as ex:
            logger.error(f"Can't store job definition for sample ${sample_id}")
            return None


    def create_job(self, method, profiles, version='fixed'):
        job = {'samples': [], }

        return {}
