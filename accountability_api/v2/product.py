from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query
from accountability_api.api_utils import metadata as consts

api = Namespace("Product", path="/product", description="Get a product details")

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
    help="Please provide a valid product ID.",
)
parser.add_argument(
    "end",
    dest="end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid product ID.",
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


@api.route("/")
class Product(Resource):
    @api.expect(parser)
    def get(self):
        """
        Get a product based on provided ID.
        """
        args = parser.parse_args()
        docs = query.get_product(args.get("product_id"))

        return docs


@api.route("/list")
class ListProducts(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Product files.
        """
        product_indexes = consts.PRODUCT_TYPE_TO_INDEX
        return product_indexes


@api.route("/list/count")
class ListProductCounts(Resource):
    @api.expect(parser)
    def get(self):
        """
        Retrieve all filetypes and their indexes that we currently consider to be Ancillary files.
        """
        args = parser.parse_args()

        product_indexes = consts.PRODUCT_TYPE_TO_INDEX
        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        count = query.get_num_docs(
            product_indexes, start=start_datetime, end=end_datetime
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
        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        size = args.get("size", 40)

        if index_name in consts.PRODUCT_TYPE_TO_INDEX:
            index = consts.PRODUCT_TYPE_TO_INDEX[index_name]
            docs.extend(
                query.get_docs(index, start=start_datetime, end=end_datetime, size=size)
            )
        return docs
