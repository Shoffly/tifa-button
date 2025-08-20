import streamlit as st
import sys
import os
import json
import time
from datetime import datetime
import logging
import requests
import pandas as pd
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure page
st.set_page_config(
    page_title="Flash Sale Posts Generator",
    page_icon="🚗",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .status-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
    .button-container {
        text-align: center;
        margin: 2rem 0;
    }
    .stButton > button {
        width: 300px;
        height: 60px;
        font-size: 1.2rem;
        font-weight: bold;
        background-color: #ff6b6b;
        color: white;
        border: none;
        border-radius: 10px;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        background-color: #ff5252;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

# Webhook endpoint for sending posts
WEBHOOK_ENDPOINT = "https://anasalaa.app.n8n.cloud/webhook/6443b1e8-366d-4065-b2e0-73dceab9b820"

# Post template for flash sale cars
POST_TEMPLATE = """🚗 {make} {model} - {year} - {kilometers:,} كم
دلوقتي نزلت على التطبيق بسعر خاص لمدة 36 ساعة بس!
📋 التقرير المفصل متوفر على التطبيق.
📲 شوفها هنا: {tracking_link}"""


class FlashSalePostGenerator:
    def __init__(self):
        st.info("🚀 Initializing Flash Sale Post Generator")

        # Initialize BigQuery client using service account credentials
        self.client = self._get_bigquery_client()

        if not self.client:
            st.error("❌ Failed to initialize BigQuery client")
            raise Exception("BigQuery client initialization failed")

        st.success("✅ Flash Sale Post Generator initialized successfully")
    
    def validate_query_columns(self, query: str) -> tuple[bool, list]:
        """
        Validate that the query contains the required column names
        Returns: (is_valid, missing_columns)
        """
        required_columns = [
            'sf_vehicle_name',
            'ajans_vehicle_id', 
            'published_at',
            'car_make',
            'car_model',
            'car_year',
            'kilometrage'
        ]
        
        # Convert query to lowercase for case-insensitive checking
        query_lower = query.lower()
        missing_columns = []
        
        for column in required_columns:
            # Check if column name appears in the query (as alias or column name)
            if column.lower() not in query_lower:
                missing_columns.append(column)
        
        is_valid = len(missing_columns) == 0
        return is_valid, missing_columns

    def get_credentials(self):
        """Function to get BigQuery credentials"""
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
            st.success("✅ Successfully loaded credentials from Streamlit secrets")
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
                st.success("✅ Successfully loaded service_account.json for BigQuery credentials")
            except FileNotFoundError:
                st.error("❌ No credentials found for BigQuery access")
                return None
        return credentials

    def _get_bigquery_client(self):
        """Get BigQuery client with service account credentials"""
        st.info("🔐 Attempting to initialize BigQuery client")

        credentials = self.get_credentials()
        if not credentials:
            return None

        try:
            # Create a BigQuery client using the credentials
            client = bigquery.Client(credentials=credentials)
            st.success("✅ BigQuery client initialized successfully")
            return client
        except Exception as e:
            st.error(f"❌ Error creating BigQuery client: {e}")
            return None

    def get_flash_sale_cars(self, custom_query: str = None) -> List[Dict]:
        """
        Get flash sale cars from BigQuery using the provided query
        """
        st.info("🔍 Fetching wholesale-to-retail published cars from BigQuery")

        if not self.client:
            st.error("❌ No BigQuery client available")
            return []

        # Use custom query if provided, otherwise use default
        if custom_query:
            # Validate the custom query has required columns
            is_valid, missing_columns = self.validate_query_columns(custom_query)
            if not is_valid:
                st.error(f"❌ Custom query is missing required columns: {', '.join(missing_columns)}")
                st.error("Please ensure your query returns all required columns with exact names.")
                return []
            
            query = custom_query
            st.info("✅ Using custom query provided by user")
        else:
            # Default query
            query = """
                    SELECT DISTINCT a.car_name as sf_vehicle_name,
                    a.vehicle_id as ajans_vehicle_id,
                    DATE(a.log_date) AS published_at,
                    b.car_make,
                    b.car_model,
                    b.car_year,
                    b.kilometrage
    FROM ajans_dealers.wholesale_vehicle_activity_logs a 
    LEFT JOIN reporting.vehicle_acquisition_to_selling b ON a.car_name = b.car_name
    WHERE DATE(log_date) = current_date() AND status_before = "created" AND status_after = "published"
            """
            st.info("📝 Using default query")

        try:
            st.info("Executing flash sale cars query")
            result = self.client.query(query).to_dataframe()

            st.success(f"📊 Found {len(result)} wholesale-to-retail published cars")

            if result.empty:
                st.warning("⚠️ No wholesale-to-retail published cars found for today")
                return []

            # Convert to list of dictionaries
            cars = []
            for _, row in result.iterrows():
                car_data = {
                    'sf_vehicle_name': row['sf_vehicle_name'],
                    'ajans_vehicle_id': row['ajans_vehicle_id'],
                    'make': row['car_make'] if not pd.isna(row['car_make']) else 'Unknown',
                    'model': row['car_model'] if not pd.isna(row['car_model']) else 'Unknown',
                    'year': int(row['car_year']) if not pd.isna(row['car_year']) else 0,
                    'kilometers': int(row['kilometrage']) if not pd.isna(row['kilometrage']) else 0,
                    'published_at': row['published_at']
                }
                cars.append(car_data)
                st.info(
                    f"🚗 {car_data['sf_vehicle_name']} (ID: {car_data['ajans_vehicle_id']}): {car_data['make']} {car_data['model']} {car_data['year']}")

            st.success(f"✅ Successfully processed {len(cars)} wholesale-to-retail published cars")
            return cars

        except Exception as e:
            st.error(f"❌ Error getting wholesale-to-retail published cars: {e}")
            return []

    def create_tracking_link(self, vehicle_name: str, ajans_vehicle_id: str) -> str:
        """
        Create a tracking link for the car using nonito.xyz API
        Based on the abandon car flow implementation

        Args:
            vehicle_name: The vehicle name (sf_vehicle_name)
            ajans_vehicle_id: The ajans vehicle ID for the deeplink

        Returns:
            Tracking link URL or fallback URL if creation fails
        """
        st.info(f"🔗 Creating tracking link for vehicle {vehicle_name}")

        # Generate link name in format: {flashsale-date-cname}
        current_date = datetime.now().strftime("%Y%m%d")
        link_name = f"flashsale-{current_date}-{vehicle_name}"

        # API endpoint and payload
        url = "https://hi.nonito.xyz/create_tracking_link"
        payload = {
            "link_name": link_name,
            "final_link": f"sylndr://car-details/{ajans_vehicle_id}",
            "sender_id": "6eb7c941-13b9-4eb3-8402-5310e2bc0f8e",
            "domain": "elajans.link",
            "deeplink": True
        }

        try:
            st.info(f"🔗 Making tracking link request: {json.dumps(payload, indent=2)}")
            response = requests.post(url, json=payload, timeout=30)

            st.info(f"🔗 Tracking link API Response - Status Code: {response.status_code}")
            st.info(f"🔗 Tracking link API Response Body: {response.text}")

            if response.status_code == 200:
                response_data = response.json()
                # Assuming the API returns the tracking link in the response
                tracking_link = response_data.get('tracking_link') or response_data.get('link') or response_data.get(
                    'url')
                if tracking_link:
                    st.success(f"✅ Tracking link created successfully: {tracking_link}")
                    return tracking_link
                else:
                    st.warning(f"⚠️ Tracking link API returned success but no link found in response")
                    return "elajans.link"
            else:
                st.error(f"❌ Tracking link creation failed with status {response.status_code}: {response.text}")
                return "elajans.link"

        except requests.exceptions.RequestException as e:
            st.error(f"❌ Tracking link request failed with exception: {e}")
            return "elajans.link"
        except Exception as e:
            st.error(f"❌ Unexpected error creating tracking link: {e}")
            return "elajans.link"

    def generate_post_content(self, car_data: Dict, tracking_link: str) -> str:
        """
        Generate post content using the template and car data
        """
        st.info(f"📝 Generating post content for {car_data['sf_vehicle_name']}")

        try:
            post_content = POST_TEMPLATE.format(
                make=car_data['make'],
                model=car_data['model'],
                year=car_data['year'],
                kilometers=car_data['kilometers'],
                tracking_link=tracking_link
            )

            st.success(f"✅ Post content generated for {car_data['sf_vehicle_name']}")
            return post_content

        except Exception as e:
            st.error(f"❌ Error generating post content for {car_data['sf_vehicle_name']}: {e}")
            # Return a basic fallback post
            return f"🔥 Flash Sale! {car_data['make']} {car_data['model']} {car_data['year']} - {tracking_link}"

    def generate_posts(self, custom_query: str = None) -> List[Dict]:
        """
        Main function to generate posts for all flash sale cars
        """
        st.info("🚀 Starting flash sale posts generation...")

        # Get flash sale cars
        flash_sale_cars = self.get_flash_sale_cars(custom_query)

        if not flash_sale_cars:
            st.info("ℹ️ No flash sale cars found. No posts to generate.")
            return []

        st.info(f"📱 Generating posts for {len(flash_sale_cars)} flash sale cars")

        posts = []
        successful_posts = 0
        failed_posts = 0

        for car_data in flash_sale_cars:
            vehicle_name = car_data['sf_vehicle_name']

            try:
                st.info(
                    f"🚗 Processing {vehicle_name} (ID: {car_data['ajans_vehicle_id']}): {car_data['make']} {car_data['model']} {car_data['year']}")

                # Skip cars with unknown make or model
                if car_data['make'] == 'Unknown' or car_data['model'] == 'Unknown':
                    st.warning(f"⏭️ Skipping {vehicle_name}: Unknown make or model")
                    failed_posts += 1
                    continue

                # Use ajans_vehicle_id as the listing ID for deeplink
                ajans_vehicle_id = car_data['ajans_vehicle_id']
                if not ajans_vehicle_id:
                    st.warning(f"⚠️ No ajans_vehicle_id found for {vehicle_name}, skipping")
                    failed_posts += 1
                    continue

                # Create tracking link using ajans_vehicle_id
                tracking_link = self.create_tracking_link(vehicle_name, ajans_vehicle_id)

                # Generate post content
                post_content = self.generate_post_content(car_data, tracking_link)

                # Add to posts list
                post_data = {
                    "car_id": vehicle_name,
                    "ajans_vehicle_id": ajans_vehicle_id,
                    "make": car_data['make'],
                    "model": car_data['model'],
                    "year": car_data['year'],
                    "kilometers": car_data['kilometers'],
                    "tracking_link": tracking_link,
                    "post_content": post_content,
                    "generated_at": datetime.now().isoformat()
                }

                posts.append(post_data)
                successful_posts += 1

                st.success(f"✅ Successfully generated post for {vehicle_name}")

            except Exception as e:
                st.error(f"❌ Error processing {vehicle_name}: {e}")
                failed_posts += 1
                continue

        # Summary
        st.info("📊 FLASH SALE POSTS GENERATION SUMMARY")
        st.info(f"🚗 Total flash sale cars found: {len(flash_sale_cars)}")
        st.info(f"✅ Posts generated successfully: {successful_posts}")
        st.info(f"❌ Posts failed: {failed_posts}")
        st.success("🏁 Flash sale posts generation completed!")

        return posts

    def send_posts_to_webhook(self, posts: List[Dict]) -> bool:
        """
        Send posts as a flat dictionary payload to the webhook endpoint
        """
        st.info(f"🌐 Sending {len(posts)} posts to webhook endpoint")
        st.info(f"🔗 Webhook URL: {WEBHOOK_ENDPOINT}")

        if not posts:
            st.warning("⚠️ No posts to send to webhook")
            return False

        # Prepare payload as a flat dictionary with each post as a separate key
        payload = {}

        # Add metadata
        payload["total_count"] = len(posts)
        payload["generated_at"] = datetime.now().isoformat()
        payload["source"] = "flash_sale_posts_generator"

        # Add each post as a separate key in the dictionary
        for i, post in enumerate(posts):
            post_key = f"post_{i + 1}"
            payload[post_key] = post

        try:
            st.info(f"🌐 Sending webhook request with {len(posts)} posts")

            # Send POST request to webhook
            response = requests.post(
                WEBHOOK_ENDPOINT,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "FlashSalePostGenerator/1.0"
                },
                timeout=30
            )

            st.info(f"🌐 Webhook Response - Status Code: {response.status_code}")
            st.info(f"🌐 Webhook Response Body: {response.text}")

            if response.status_code in [200, 201, 202]:
                st.success(f"✅ Successfully sent {len(posts)} posts to webhook")
                return True
            else:
                st.error(f"❌ Webhook request failed with status {response.status_code}: {response.text}")
                return False

        except requests.exceptions.Timeout:
            st.error("❌ Webhook request timed out")
            return False
        except requests.exceptions.RequestException as e:
            st.error(f"❌ Webhook request failed with exception: {e}")
            return False
        except Exception as e:
            st.error(f"❌ Unexpected error sending to webhook: {e}")
            return False


def run_flash_sale_generation():
    """Run the flash sale posts generation process"""
    try:
        # Get custom query from session state
        custom_query = st.session_state.get('custom_query', None)
        
        # Initialize the post generator
        st.info("🔧 Initializing Flash Sale Post Generator...")
        generator = FlashSalePostGenerator()

        # Generate posts
        st.info("📱 Generating flash sale posts...")
        posts = generator.generate_posts(custom_query)

        if posts:
            # Send to webhook endpoint
            st.info("🌐 Sending posts to webhook...")
            webhook_success = generator.send_posts_to_webhook(posts)

            # Also print to console for immediate use
            st.info("📄 Generated posts JSON:")
            st.json(posts)

            st.success(f"✅ Successfully generated {len(posts)} posts!")
            if webhook_success:
                st.success("🌐 ✅ Posts sent to webhook successfully!")
            else:
                st.error("🌐 ❌ Failed to send posts to webhook")

            return {
                'success': True,
                'posts': posts,
                'webhook_success': webhook_success,
                'total_posts': len(posts)
            }
        else:
            st.info("ℹ️ No posts generated - no flash sale cars found or all failed processing")
            return {
                'success': True,
                'posts': [],
                'webhook_success': False,
                'total_posts': 0
            }

    except Exception as e:
        st.error(f"💥 Critical error in flash sale posts generation: {e}")
        return {
            'success': False,
            'posts': [],
            'webhook_success': False,
            'total_posts': 0,
            'error': str(e)
        }


def main():
    # Header
    st.markdown('<h1 class="main-header">🚗 Flash Sale Posts Generator</h1>', unsafe_allow_html=True)

    # Description
    st.markdown("""
    This app generates automated social media posts for flash sale cars by:
    - Fetching flash sale cars from BigQuery
    - Creating tracking deeplinks for each car
    - Generating post content using templates
    - Sending posts to webhook endpoint
    """)

    # Query Configuration Section
    st.markdown("## ⚙️ Query Configuration")
    
    # Default query
    default_query = """SELECT DISTINCT a.car_name as sf_vehicle_name,
a.vehicle_id as ajans_vehicle_id,
DATE(a.log_date) AS published_at,
b.car_make,
b.car_model,
b.car_year,
b.kilometrage
FROM ajans_dealers.wholesale_vehicle_activity_logs a 
LEFT JOIN reporting.vehicle_acquisition_to_selling b ON a.car_name = b.car_name
WHERE DATE(log_date) = current_date() AND status_before = "created" AND status_after = "published\""""
    
    # Instructions
    st.markdown("""
    **⚠️ Required Column Names:**
    Your query MUST return these exact column names for the script to work:
    - `sf_vehicle_name` - Vehicle name/identifier
    - `ajans_vehicle_id` - Vehicle ID for deep linking
    - `published_at` - Publication date
    - `car_make` - Car manufacturer
    - `car_model` - Car model
    - `car_year` - Car year
    - `kilometrage` - Car mileage/kilometers
    """)
    
    # Query input
    custom_query = st.text_area(
        "📝 BigQuery SQL (modify as needed):",
        value=default_query,
        height=200,
        help="Modify this query but ensure it returns all required columns with exact names listed above"
    )
    
    # Store the query in session state
    st.session_state['custom_query'] = custom_query
    
    # Query validation preview
    if st.button("🔍 Validate Query", key="validate_query"):
        st.markdown("### 🔍 Query Validation Results")
        
        # Create a temporary generator to use validation method
        try:
            temp_generator = FlashSalePostGenerator()
            is_valid, missing_columns = temp_generator.validate_query_columns(custom_query)
            
            if is_valid:
                st.success("✅ Query validation passed! All required columns are present.")
            else:
                st.error(f"❌ Query validation failed! Missing columns: {', '.join(missing_columns)}")
                st.info("Please ensure your query returns all required columns with exact names.")
        except Exception as e:
            st.error(f"❌ Error during validation: {e}")

    

    # Button container
    st.markdown('<div class="button-container">', unsafe_allow_html=True)

    # Generate button
    if st.button("🚀 Generate Flash Sale Posts & Send Webhook", key="generate_button"):
        st.markdown('</div>', unsafe_allow_html=True)

        # Create progress indicators
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Update progress
        status_text.text("🔄 Initializing Flash Sale Posts Generator...")
        progress_bar.progress(10)
        time.sleep(0.5)

        status_text.text("🔍 Fetching flash sale cars from BigQuery...")
        progress_bar.progress(30)
        time.sleep(0.5)

        status_text.text("🔗 Creating tracking links...")
        progress_bar.progress(50)
        time.sleep(0.5)

        status_text.text("📝 Generating post content...")
        progress_bar.progress(70)
        time.sleep(0.5)

        status_text.text("🌐 Sending posts to webhook...")
        progress_bar.progress(90)
        time.sleep(0.5)

        # Run the generation process
        status_text.text("⚡ Executing generation process...")
        result = run_flash_sale_generation()

        # Complete progress
        progress_bar.progress(100)
        status_text.text("✅ Complete!")
        time.sleep(1)

        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()

        # Display results
        st.markdown("## 📊 Execution Results")

        if result['success']:
            st.markdown('<div class="status-box success-box">', unsafe_allow_html=True)
            st.success("✅ Flash sale posts generation completed successfully!")
            st.markdown('</div>', unsafe_allow_html=True)

            if result['posts']:
                # Summary
                st.markdown("## 📈 Summary")
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Total Posts", result['total_posts'])

                with col2:
                    st.metric("Generated At", datetime.now().strftime("%H:%M:%S"))

                with col3:
                    status = "✅ Sent to Webhook" if result['webhook_success'] else "❌ Webhook Failed"
                    st.metric("Status", status)
            else:
                st.info("ℹ️ No posts were generated - no flash sale cars found for today")

        else:
            st.markdown('<div class="status-box error-box">', unsafe_allow_html=True)
            st.error("❌ Flash sale posts generation failed!")
            if 'error' in result:
                st.error(f"Error: {result['error']}")
            st.markdown('</div>', unsafe_allow_html=True)

    else:
        st.markdown('</div>', unsafe_allow_html=True)

        
        
        # Show current time
        st.markdown(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
