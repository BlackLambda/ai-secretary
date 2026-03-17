import sys
import os
import json
import logging
import re

# Add current directory to path so we can import from src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.profiles import ProfileService
from src.utils.json_writer import save_json

def parse_profile_string(profile_str):
    """Parses the raw profile string into a structured dictionary."""
    if not profile_str:
        return {}

    parsed_data = {}
    # The profile string seems to be one large string with \n separators
    lines = profile_str.strip().split('\n')

    for line in lines:
        line = line.strip()
        if line.startswith('- '):
            content = line[2:]
            if ':' in content:
                key, value = content.split(':', 1)
                key = key.strip()
                value = value.strip()

                # Check for list format like [Item 1],[Item 2]
                if value.startswith('[') and value.endswith(']'):
                    # Split by "],[" to handle items
                    items = value.split('],[')
                    cleaned_items = []
                    for i, item in enumerate(items):
                        # Clean leading [ for first item
                        if i == 0 and item.startswith('['):
                            item = item[1:]
                        # Clean trailing ] for last item
                        if i == len(items) - 1 and item.endswith(']'):
                            item = item[:-1]
                        cleaned_items.append(item)
                    parsed_data[key] = cleaned_items
                
                # Check for comma separated lists
                elif ',' in value:
                    # Heuristic: keys that sound like lists or have multiple commas
                    list_indicators = ["contacts", "authors", "modifiers", "phrases", "topics"]
                    if any(indicator in key.lower() for indicator in list_indicators) or value.count(',') > 0:
                        parsed_data[key] = [v.strip() for v in value.split(',')]
                    else:
                        parsed_data[key] = value
                else:
                    parsed_data[key] = value
                    
    return parsed_data

def main():
    # Setup basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    logging.info("Fetching user profile...")
    
    try:
        service = ProfileService()
        profile_data = service.get_profile()
        
        # Get UPN and Alias
        upn = service.client.upn
        alias = upn.split('@')[0] if upn else "unknown"
        
        if profile_data and "user_profile" in profile_data:
            raw_profile = profile_data["user_profile"]
            
            # Parse the raw string
            parsed_data = parse_profile_string(raw_profile)
            
            # Fetch extended profile info (Department, etc.) directly
            extended_profile = service.get_extended_profile()
            
            # Create formatted profile with extended info at the top
            formatted_profile = {}
            if extended_profile:
                formatted_profile.update(extended_profile)
            
            # Add parsed data
            formatted_profile.update(parsed_data)
            
            # Combine raw and formatted data
            result = {
                "upn": upn,
                "alias": alias,
                "raw_profile": raw_profile,
                "formatted_profile": formatted_profile
            }
            
            # Print formatted to console
            print(f"User: {upn}")
            print(f"Alias: {alias}")
            if extended_profile:
                print(f"Department: {extended_profile.get('department')}")
                print(f"Office: {extended_profile.get('officeLocation')}")
                print(f"Job Title: {extended_profile.get('jobTitle')}")
            print("-" * 40)
            print(json.dumps(formatted_profile, indent=2))
            
            # Save to file
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            save_json(result, "user_profile.json", output_dir)
            logging.info(f"Profile saved to {output_dir}/user_profile.json")
        else:
            logging.warning("No profile data returned.")
            
    except Exception as e:
        logging.error(f"Failed to get profile: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
