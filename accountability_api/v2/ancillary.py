from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query
from accountability_api.api_utils import metadata as consts

api = Namespace(
    "Ancillary", path="/ancillary", description="Get a ancillary file details"
)

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
    "size",
    default=40,
    dest="size",
    type=int,
    location="args",
    required=False,
    help="Please provide a valid size.",
)


@api.route("/list")
class ListAncillaries(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Ancillary files.
        """
        ancillary_indexes = consts.ANCILLARY_INDEXES
        return ancillary_indexes


@api.route("/list/count")
class ListAncillaryCounts(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Ancillary files.
        """
        args = parser.parse_args()

        ancillary_indexes = consts.ANCILLARY_INDEXES
        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        count = query.get_num_docs(
            ancillary_indexes, start=start_datetime, end=end_datetime
        )
        results = []
        for c in count:
            results.append({"id": c, "count": count[c]})
        return results


@api.route("/<path:index_name>")
class AncillaryIndex(Resource):
    @api.expect(parser)
    def get(self, index_name):
        """
        Get a product based on provided ID.
        """
        args = parser.parse_args()
        docs = []
        if args.get("product_id"):
            docs = query.get_product(args.get("product_id"))
        else:
            start_datetime = args.get("start_datetime", None)
            end_datetime = args.get("end_datetime", None)
            size = args.get("size", 40)

            if index_name in consts.ANCILLARY_INDEXES:
                index = consts.ANCILLARY_INDEXES[index_name]
                docs.extend(
                    query.get_docs(index, start_datetime, end_datetime, size=size)
                )
                # retrieve docs for each index

        return docs


@api.route("/")
class Ancillary(Resource):
    @api.expect(parser)
    def get(self):
        """
        Get ancillary elasticsearch entries.
        """
        args = parser.parse_args()
        docs = []

        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        size = args.get("size", 40)

        for name in consts.ANCILLARY_INDEXES:
            index = consts.ANCILLARY_INDEXES[name]

            docs.extend(query.get_docs(index, start_datetime, end_datetime, size=size))
            # retrieve docs for each index

        return docs
