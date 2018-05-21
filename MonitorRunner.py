import yamlconf
import rest_clients
from monitor.Monitor import Monitor
from multiprocessing import JoinableQueue

if __name__ == "__main__":
    with open("appconfig.yml", 'r') as stream:
        config = yamlconf.load(stream)

    zipping_q = JoinableQueue()
    conversion_q = JoinableQueue()

    stasis_cli = rest_clients.StasisClient(config['stasis']['url'])
    dataform_cli = rest_clients.DataformerClient(config['dataform']['url'], config['dataform']['port'], config['dataform']['storage'])

    monitor = Monitor(config, stasis_cli, dataform_cli, zipping_q, conversion_q)
    try:
        monitor.start()
    except Exception as ex:
        print(ex.args)
