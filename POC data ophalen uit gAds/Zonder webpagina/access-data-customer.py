import argparse
import sys

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def main(client, customer_id):
    client.login_customer_id = "xxxxxxxxxxxxx"
    ga_service = client.get_service("GoogleAdsService")
    query = """
            SELECT
                campaign.name,
                campaign.id,
                campaign.start_date,
                campaign.tracking_url_template
            FROM campaign
            WHERE campaign.resource_name = 'customers/xxxxxxxxxxx/campaigns/xxxxxxxxx'
        """

    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query
    response = ga_service.search(request=request)

    for row in response:
        customer = row.campaign
        print(f"Name: {customer.name}")
        print(f"ID: {customer.id}")
        print(f"Start date: {customer.start_date}")
        print(f"Budget: {customer.tracking_url_template}")
        print("\n")


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v13")

    parser = argparse.ArgumentParser(
        description=(
            "Displays billing setup information for the specified "
            "Google Ads customer account."
        )
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=True,
        help="The Google Ads customer ID.",
    )
    args = parser.parse_args()

    try:
        main(googleads_client, args.customer_id)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
