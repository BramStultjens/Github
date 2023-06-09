from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    manager_customer_id = "xxxxxxxxx"  # Replace with your manager customer ID.
    output_file_path = "output.csv"  # Replace with the path to your output file.
    list_accounts(client, manager_customer_id, output_file_path)

def list_accounts(client, manager_customer_id, output_file_path):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch all managed accounts.
    query = f'''
        SELECT customer_client.client_customer,
            customer_client.descriptive_name,
            customer_client.currency_code,
            customer_client.time_zone
        FROM customer_client
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=manager_customer_id, query=query)

    for row in response:
        customer = row.customer_client
        print(f'Managed account with ID "{customer.client_customer}", '
            f'name "{customer.descriptive_name}", '
            f'time zone "{customer.time_zone}" was found.')
  
if __name__ == '__main__':
    main()