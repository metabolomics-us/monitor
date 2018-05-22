import yamlconf

from monitor.Monitor import Monitor
from rest.dataformer.DataformerClient import DataformerClient
from rest.stasis.StasisClient import StasisClient

if __name__ == "__main__":
    with open("appconfig.yaml", 'r') as stream:
        config = yamlconf.load(stream)

    stasis_cli = StasisClient(config['stasis']['url'])
    dataform_cli = DataformerClient(
        config['dataform']['url'],
        config['dataform']['port'],
        config['monitor']['storage']
    )

    monitor = Monitor(config, stasis_cli, dataform_cli)

    try:
        monitor.start()
    except Exception as ex:
        print(ex.args)
