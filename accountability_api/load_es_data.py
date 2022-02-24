import os
import sys
import json

from api_utils import connect_to_es

local_grq_es = connect_to_es("localhost:9200")
local_mozart_es = connect_to_es("localhost:9300")

GRQ_ES_URL = "http://localhost:9200/"
MOZART_ES_URL = "http://localhost:9300/"


def restore(backup_dir, es_docker_name="grq"):
    """Restore ES index from backup docs and mapping."""

    # get files
    docs_files = os.listdir(backup_dir)
    local_es = None

    if es_docker_name == "grq":
        local_es = local_grq_es
    elif es_docker_name == "mozart":
        local_es = local_mozart_es
    else:
        raise Exception("Could not find requested es docker instance")

    local_es.cluster.health(wait_for_status="yellow", timeout="50s")

    for index in docs_files:
        # delete all indexes
        try:
            local_es.indices.delete(index)
        except Exception:
            pass

        local_es.indices.create(index)

        path = backup_dir + index + "/docs"
        docs = os.listdir(path)
        for doc in docs:
            j = None
            with open(path + "/" + doc) as f:
                j = json.load(f)
                try:
                    local_es.create(index, doc.split(".")[0], j, doc_type="_doc")
                except Exception:
                    pass


if __name__ == "__main__":
    backup_idx_dir = sys.argv[1]
    es_docker_name = "grq"
    if len(sys.argv) > 2:
        es_docker_name = sys.argv[2]
    restore(backup_idx_dir, es_docker_name=es_docker_name)
