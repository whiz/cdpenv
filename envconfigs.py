import sys

from cdpy.cdpy import Cdpy
from cdpy.common import  CdpcliWrapper
from cdpy.environments import  CdpyEnvironments
from cdpy.datalake import CdpyDatalake
from cdpy.datahub import CdpyDatahub
from cdpy.dw import CdpyDw


if __name__ == '__main__':
    args = sys.argv[1:]
    message = ""
    if args:
        envname = args[0]
    else:
        message = "Use Environment Name"
        print(message)
        sys.exit(1)
    #envname = 'se-sandbox-aws'
    environment = CdpyEnvironments()
    datalake = CdpyDatalake()
    datahub = CdpyDatahub()
    datawarehouse = CdpyDw()
    environmentJSON = environment.describe_environment(envname)
    environmentCRN = environmentJSON['crn']

    outputJSON = environmentJSON



    idbrokermappings  = environment.get_id_broker_mappings(envname)

    outputJSON['IDBrokerMappings'] = idbrokermappings



    #datalakes = datalake.list_datalakes(envname)

    #datalakeJSON = datalake.describe_datalake(datalakes[0])
    datalakeJSON = datalake.describe_all_datalakes(envname)

    outputJSON['DataLakes'] = datalakeJSON

    datahubList = datahub.list_clusters(envname)

    datahubJSON = datahub.describe_all_clusters(envname)

    outputJSON['Datahubs'] = datahubJSON
    clusters = datawarehouse.list_clusters(env_crn=environmentCRN)
    out = []
    if clusters:
        for base in clusters:
            clusterId = base['id']

            dbcs = datawarehouse.list_dbcs(clusterId)
            vws = datawarehouse.list_vws(clusterId)

            #CdpcliWrapper().call(svc='dw', func='list-vw-configs', )
            dbcJSON = []
            vwJSON = []
            if dbcs:
                for dbc in dbcs:
                    dbcId = dbc['id']
                    dbcJSON.append(datawarehouse.describe_dbc(clusterId, dbcId))

            if vws:
                for vw in vws:
                    vwId = vw['id']
                    vwJSON.append(datawarehouse.describe_vw(base['id'], vwId))

            out.append({
                'dbcs': dbcJSON,
                'vws': vwJSON,
                **base
            })

    outputJSON['Datawarehouse'] = out

    print(outputJSON)
    outputJSON

    #print(outputJSON)
