import logging
import platform
import time
from threading import Thread
from time import sleep

import boto3
import simplejson as json
import watchtower

from monitor.QueueManager import QueueManager
from monitor.client.BackendClient import BackendClient
from monitor.exceptions import NoProfileException, SampleNotFoundException, JobDataStoreException

logger = logging.getLogger('Scheduler')
h = watchtower.CloudWatchLogHandler(
    log_group_name=platform.node(),
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)

class Scheduler(Thread):
    """
    Worker class that schedules samples for preprocessing on AWS
    """

    def __init__(self, parent, backend_cli: BackendClient,
                 config, queue_mgr: QueueManager,
                 name='Scheduler0', daemon=True):
        """
        Args:
            parent:
                Instance parent object
            backend_cli: BackendClient
                A stasis client instance
            config:
                An object containing config settings
            queue_mgr: QueueManager
                A QueueManager object that handles setting up queues and sending/receiving messages
            name: str (Optional. Default: Scheduler0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=name, daemon=daemon)
        self.sqs = boto3.client('sqs')

        self.parent = parent
        self.running = False
        self.queue_mgr = queue_mgr
        self.backend_cli = backend_cli
        self.schedule = config['monitor']['schedule']
        self.test = config['test']

    def run(self):
        item = None
        self.running = True

        while self.running:
            try:
                item = self.queue_mgr.get_next_message(self.queue_mgr.process_q())
                if not item:
                    logger.debug('\twaiting...')
                    time.sleep(2.7)
                    continue

                logger.info(f'Scheduling sample with id {item}')

                job = self.schedule_sample(item)
                logger.info(f'\tJob id: {job}')

                if job:
                    logger.info(f'\tSchedule successful. Job id {job["job"]}')
                else:
                    self.fail_sample(item, reason='Error getting or creating job to schedule sample.', extension="")

            except KeyboardInterrupt:
                logger.warning(f'\tStopping {self.name} due to Keyboard Interrupt')
                self.running = False
                self.parent.join_threads()

            except IndexError:
                sleep(1)

            except SampleNotFoundException:
                msg = f'\tAcquisition data for sample {item} not found'
                logger.error(msg, exc_info=True)
                self.fail_sample(item, '', reason=msg)

            except NoProfileException as ex:
                msg = f'\tScheduling error: {ex.args}'
                logger.error(msg, exc_info=True)
                self.fail_sample(item, '', reason=msg)

            except JobDataStoreException as ex:
                msg = f'\tError scheduling job {ex.args}'
                logger.error(msg, exc_info=True)
                self.fail_sample(item, '', reason=msg)

            except Exception as ex:
                msg = f'\tError scheduling sample {item}: {ex.args}'
                logger.error(msg, exc_info=True)
                self.fail_sample(item, '', reason=msg)

            finally:
                logger.info(f'Scheduler queue size: {self.queue_mgr.get_size(self.queue_mgr.process_q())}')

        logger.info(f'Stopping {self.name}')
        self.join()

    def schedule_sample(self, sample_id):
        sample_data = {}

        try:
            # get sample data.
            sample_data = self.backend_cli.sample_acquisition_get(sample_id)

            logger.debug(json.dumps(sample_data, indent=2))

        except Exception as ex:
            if 'acquisition data not found' in str(ex):
                raise SampleNotFoundException(sample_id)
            else:
                logger.error(f"\tCan't get sample '{sample_id}' metadata. StasisClient error: {ex.args}",
                              exc_info=True)
                raise ex

        # build method string
        method = ' | '.join([sample_data['chromatography']['method'], sample_data['chromatography']['instrument'],
                             sample_data['chromatography']['column'], sample_data['chromatography']['ionisation']])

        version = self._get_latest_version(method)
        logger.debug(f'\tlatest method version: ' + version)

        # get profile data
        profile_list = self.backend_cli.get_unique_method_profiles(method, version)
        logger.debug(f'\tProfiles: {profile_list}')

        if len(profile_list) <= 0:
            raise NoProfileException(method, version)

        profiles = ','.join([x['profile'] for x in profile_list])

        job = {
            'id': f'preprocess_{sample_id}',  # in case we need more uniqueness: {time.strftime("%Y%m%d-%H%M%S")}_
            'method': method,
            'profile': profiles,
            'samples': [sample_id]
        }

        logger.debug('\tstoring job...')
        stored_samples = self.backend_cli.store_job(job)

        logger.info(f"\tJob {job['id']} stored successfully.")

        if stored_samples:
            response = self.backend_cli.schedule_job(job['id'])

            logger.info(f"\tSample {sample_id} scheduled successfully.")
            return response
        else:
            raise JobDataStoreException(job['id'])

    def _get_latest_version(self, method):
        versions = self.backend_cli.get_method_last_version(method)

        def get_version(item):
            return item['updated']

        if not versions:
            return 'fixed'
        else:
            vsorted = sorted(versions, reverse=True, key=get_version)

            return vsorted[0]['version']

    def fail_sample(self, file_basename, extension, reason: str):
        try:
            logger.error(f'\tAdd "failed" scheduling status to stasis for sample "{file_basename}"')
            self.backend_cli.sample_state_update(file_basename, 'failed',
                                                file_handle=f'{file_basename}.{extension}',
                                                reason=reason)
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                          f'\tResponse: {str(ex)}')
