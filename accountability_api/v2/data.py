from typing import List

from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query
from accountability_api.api_utils import metadata as consts
from accountability_api.api_utils.utils import set_transfer_status

api = Namespace("All Data", path="/data", description="Get all data details")

parser = reqparse.RequestParser()
parser.add_argument(
    "id",
    dest="product_id",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid product ID.",
)
parser.add_argument(
    "start",
    dest="start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "end",
    dest="end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "workflow_start",
    dest="workflow_start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "workflow_end",
    dest="workflow_end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "obs_start",
    dest="obs_start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "obs_end",
    dest="obs_end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime",
)
parser.add_argument(
    "size",
    default=40,
    dest="size",
    type=int,
    location="args",
    required=False,
    help="Please provide a valid size.",
)
parser.add_argument(
    "source",
    dest="source",
    type=int,
    location="args",
    required=False,
    help="Please provide a valid size.",
)
parser.add_argument(
    "category",
    dest="category",
    type=str,
    location="args",
    required=False,
    help="Data category. ( incoming | outgoing | all )",
)
parser.add_argument(
    "metadata.tile_id",
    dest="metadata_tile_id",
    type=str,
    location="args",
    required=False,
    help="Tile ID."
)


@api.route("/list")
class ListDataTypes(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Ancillary files.
        """
        indexes = {}
        indexes.update(consts.ANCILLARY_INDEXES)
        indexes.update(consts.PRODUCT_INDEXES)

        return indexes


@api.route("/list/count")
class ListDataTypeCounts(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Ancillary files.
        """
        args = parser.parse_args()

        indexes = {}
        if args.get('category') == 'incoming':
            indexes.update(consts.INCOMING_SDP_PRODUCTS)
        elif args.get('category') == 'outgoing':
            indexes.update(consts.OUTGOING_PRODUCTS_TO_DAAC)
        else:
            indexes.update(consts.INCOMING_SDP_PRODUCTS)
            indexes.update(consts.OUTGOING_PRODUCTS_TO_DAAC)

        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        workflow_start_datetime = args.get("workflow_start", None)
        workflow_end_datetime = args.get("workflow_end", None)
        count = query.get_num_docs(
            indexes,
            start=start_datetime,
            end=end_datetime,
            workflow_start=workflow_start_datetime,
            workflow_end=workflow_end_datetime,
        )
        results = []
        for c in count:
            results.append({"id": c, "count": count[c]})
        return results


@api.route("/<path:index_name>")
class DataIndex(Resource):
    @api.expect(parser)
    def get(self, index_name):
        """
        Get a product based on provided ID.
        """
        args = parser.parse_args()
        docs = []

        indexes = {}
        indexes.update(consts.ANCILLARY_INDEXES)
        indexes.update(consts.PRODUCT_INDEXES)

        product_id = args.get("product_id", None)
        size = args.get("size", 40)

        if args.get("product_id"):
            docs = query.get_product(product_id)
        else:
            start_dt = args.get("start_datetime", None)
            end_dt = args.get("end_datetime", None)
            # workflow_start_dt = args.get("workflow_start", None)
            # workflow_end_dt = args.get("workflow_end", None)
            if not size:
                size = 40

            if index_name in indexes:
                index = indexes[index_name]
                results = query.get_docs(
                    index,
                    time_key="created_at",
                    start=start_dt,
                    end=end_dt,
                    size=size,
                    metadata_tile_id=args["metadata_tile_id"]
                    # workflow_start=workflow_start_dt,
                    # workflow_end=workflow_end_dt,
                )
                docs.extend(results)

        for i in range(len(docs)):
            docs[i] = set_transfer_status(docs[i])

        docs = minimize_docs(docs)

        return docs


@api.route("/")
class Data(Resource):
    @api.expect(parser)
    def get(self):
        """
        Get a product based on provided ID.
        """
        args = parser.parse_args()
        docs = []

        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        # to be used at a later time
        # workflow_start_dt = args.get("workflow_start", None)
        # workflow_end_dt = args.get("workflow_end", None)

        indexes = {}
        indexes.update(consts.ANCILLARY_INDEXES)
        indexes.update(consts.PRODUCT_INDEXES)

        product_id = args.get("product_id", None)
        size = args.get("size")

        if product_id is not None:
            docs = query.get_product(product_id)
        else:
            for name in indexes:
                index = indexes[name]
                if not size:
                    size = 40
                docs.extend(
                    query.get_docs(
                        index,
                        start=start_datetime,
                        end=end_datetime,
                        size=size,
                        metadata_tile_id=args["metadata_tile_id"]
                        # to be used later
                        # workflow_start=workflow_start_dt,
                        # workflow_end=workflow_end_dt,
                    )
                )
        if len(docs) > 0:
            if not isinstance(docs, list):
                docs = [docs]
            docs = list(map(set_transfer_status, docs))

        docs = minimize_docs(docs)

        return docs


def minimize_docs(docs: List) -> List:
    """Filter out redundant data from the request"""
    for i, doc in enumerate(docs):
        docs[i] = {
            "id": doc.get("id"),
            "dataset_type": doc.get("dataset_type"),
            "metadata": {
                "FileName": doc.get("metadata", {}).get("FileName"),
                "ProductReceivedTime": doc.get("metadata", {}).get("ProductReceivedTime")
            },
            "transfer_status": doc.get("transfer_status")
        }
    return docs
