"""Script that deletes the cluster when done using it."""
from cluster_setup import config, redshift


def delete_cluster():
    """Delete the cluster when done using it."""
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get(
                'DWH', 'CLUSTER_IDENTIFIER'), SkipFinalClusterSnapshot=True)
        print('Deletion of cluster has been initiated!')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    delete_cluster()
