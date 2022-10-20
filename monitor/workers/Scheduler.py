from queue import Queue
from threading import Thread
from time import sleep

import simplejson as json
from cisclient.client import CISClient
from cisclient.exceptions import CisClientException
from loguru import logger
from stasis_client.client import StasisClient

from monitor.exceptions import NoProfileException, SampleNotFoundException, JobDataStoreException


class Scheduler(Thread):
    """
    Worker class that schedules samples for preprocessing on AWS
    """

    def __init__(self, parent, stasis: StasisClient, cis: CISClient,
                 config, sched_q: Queue,
                 name='Scheduler0', daemon=True):
        """
        Args:
            parent:
                Instance parent object
            stasis: StasisClient
                A stasis client instance
            cis: CisClient
                A cis client instance
            config:
                An object containing config settings
            sched_q: Queue
                A queue that contains the samples to be auto-scheduled
            name: str (Optional. Default: Scheduler0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.stasis_cli = stasis
        self.cis_cli = cis
        self.schedule_q = sched_q
        self.running = False
        self.schedule = config['monitor']['schedule']
        self.test = config['test']

    def run(self):
        item = None
        self.running = True

        while self.running:
            try:
                item = self.schedule_q.get()

                logger.info(f'Scheduling sample with id {item}')

                job = self.schedule_sample(item)
                logger.info(f'\tJob id: {job}')

                if job:
                    logger.info(f'\tSchedule successful. Job id {job["job"]}')
                else:
                    self.fail_sample(item, reason='Error getting or creating job to schedule sample.')

            except KeyboardInterrupt:
                logger.warning(f'\tStopping {self.name} due to Keyboard Interrupt')
                self.running = False
                self.schedule_q.queue.clear()
                self.schedule_q.join()
                self.parent.join_threads()

            except IndexError:
                sleep(1)

            except SampleNotFoundException:
                msg = f'\tAcquisition data for sample {item} not found'
                logger.error(msg, exc_info=True)
                self.fail_sample(item, '', reason=msg)

            except CisClientException as ex:
                msg = f'\tCisClient error: {ex.args}'
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
                self.schedule_q.task_done()
                logger.info(f'Scheduler queue size: {self.schedule_q.qsize()}')

        logger.info(f'Stopping {self.name}')
        self.join()

    def schedule_sample(self, sample_id):
        sample_data = {}

        try:
            # get sample data.
            sample_data = self.stasis_cli.sample_acquisition_get(sample_id)

            logger.debug(json.dumps(sample_data, indent=2))

        except Exception as ex:
            if 'acquisition data not found' in str(ex):
                raise SampleNotFoundException(sample_id)
            else:
                logger.error(f"\tCan't get sample '{sample_id}' metadata. StasisClient error: {ex.args}", exc_info=True)
                raise ex

        # build method string
        method = ' | '.join([sample_data['chromatography']['method'], sample_data['chromatography']['instrument'],
                             sample_data['chromatography']['column'], sample_data['chromatography']['ionisation']])

        version = self._get_latest_version(method)
        logger.debug(f'\tlatest method version: ' + version)

        # get profile data
        profile_list = self.cis_cli.get_unique_method_profiles(method, version)
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
        stored_samples = self.stasis_cli.store_job(job)

        logger.info(f"\tJob {job['id']} stored successfully.")

        if stored_samples:
            response = self.stasis_cli.schedule_job(job['id'])

            logger.info(f"\tSample {sample_id} scheduled successfully.")
            return response
        else:
            raise JobDataStoreException(job['id'])

    def _get_latest_version(self, method):
        versions = self.cis_cli.get_method_last_version(method)

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
            self.stasis_cli.sample_state_update(file_basename, 'failed',
                                                file_handle=f'{file_basename}.{extension}',
                                                reason=reason)
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')
