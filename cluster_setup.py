"""Script creates the cluster and retrieves the cluster's endpoint."""

import boto3
import time
import configparser

config = configparser.ConfigParser()
config.optionxform = str
config.read('dwh.cfg')

redshift = boto3.client(
    'redshift', region_name='us-west-2',
    aws_access_key_id=config.get('AWS', 'KEY'),
    aws_secret_access_key=config.get('AWS', 'SECRET')
)


def create_cluster():
    """Create the cluster and set its properties."""
    try:
        redshift.create_cluster(
            ClusterType=config.get('DWH', 'CLUSTER_TYPE'),
            NodeType=config.get('DWH', 'NODE_TYPE'),
            NumberOfNodes=int(config.get('DWH', 'NUM_NODES')),

            DBName=config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=config.get('DWH', 'CLUSTER_IDENTIFIER'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),

            IamRoles=[config.get('IAM_ROLE', 'ARN')]
        )
        print('Cluster has been created successfully!')

    except Exception as e:
        print(e)


def get_endpoint():
    """
    Retrieve the endpoint once the cluster is made available.

    Stores the endpoint value as the host in the config file.
    """
    create_cluster()
    ClusterProps = redshift.describe_clusters(
        ClusterIdentifier=config.get(
            'DWH', 'CLUSTER_IDENTIFIER'))['Clusters'][0]
    cluster_status = ClusterProps['ClusterStatus']

    print('Waiting for the cluster to be made available...')
    while cluster_status == 'creating':
        time.sleep(10)
        cluster_status = redshift.describe_clusters(
            ClusterIdentifier=config.get('DWH', 'CLUSTER_IDENTIFIER')
        )['Clusters'][0]['ClusterStatus']

        if cluster_status == 'available':
            break

    print('Cluster is now available!')
    cluster_endpoint = redshift.describe_clusters(
        ClusterIdentifier=config.get('DWH', 'CLUSTER_IDENTIFIER')
    )['Clusters'][0]['Endpoint']['Address']

    config.set('CLUSTER', 'HOST', cluster_endpoint)
    config.write(open('dwh.cfg', 'w'))
