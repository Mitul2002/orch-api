from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import os
import re
from typing import Dict, Tuple

app = FastAPI()

# Set the base path to a fixed directory
BASE_PATH = "clean/"  # Adjust this path as necessary for your project structure

class SearchRequest(BaseModel):
    target_spend: float
    carrier: str
    tolerance: float
    top_n: int

@app.get("/")
async def read_root():
    """Root endpoint returning a welcome message."""
    return {"message": "Welcome to the Contract Search API!"}

def parse_spend(spend_str: str) -> float:
    """Convert spend string (like $670K or $2.2M) to float value."""
    spend_str = spend_str.replace('$', '').replace(',', '')
    if 'M' in spend_str:
        return float(spend_str.replace('M', '')) * 1_000_000
    elif 'K' in spend_str:
        return float(spend_str.replace('K', '')) * 1_000
    return float(spend_str)

def format_spend(spend: float) -> str:
    """Convert float value back to K/M format."""
    if spend >= 1_000_000:
        return f"${spend / 1_000_000:.1f}M"
    return f"${spend / 1_000:.0f}K"

def get_spend_range(spend: float, tolerance: float) -> Tuple[float, float]:
    """Calculate spend range based on tolerance."""
    lower = spend * (1 - tolerance)
    upper = spend * (1 + tolerance)
    return lower, upper

def normalize_discount(discount: float) -> float:
    """Normalize discount value by dividing by 100 if it's greater than 100."""
    return discount / 100 if discount > 100 else discount

def search_contracts(
    target_spend: float,
    carrier: str,
    tolerance: float,
    top_n: int
) -> Dict:
    """Search contracts to find the best and worst discounts within the specified range."""
    lower_spend, upper_spend = get_spend_range(target_spend, tolerance)
    carrier_path = os.path.join(BASE_PATH, carrier)
    
    contracts_data = []
    
    # Check if the carrier directory exists
    if not os.path.exists(carrier_path):
        raise HTTPException(status_code=404, detail=f"Carrier directory '{carrier_path}' not found.")

    for filename in os.listdir(carrier_path):
        if filename.endswith('.csv'):
            spend_match = re.search(r'\$(.+?)\.csv', filename)
            if spend_match:
                contract_spend = parse_spend(spend_match.group(1))
                
                if lower_spend <= contract_spend <= upper_spend:
                    df = pd.read_csv(os.path.join(carrier_path, filename))
                    current_col = f'CURRENT {carrier.upper()}'
                    
                    contract_info = {
                        'filename': filename,
                        'spend': contract_spend,
                        'service_levels': {}
                    }
                    
                    for _, row in df.iterrows():
                        service = row['DOMESTIC AIR SERVICE LEVEL']
                        discount = normalize_discount(float(row[current_col]))
                        contract_info['service_levels'][service] = discount
                    
                    contracts_data.append(contract_info)

    service_level_stats = {}
    
    for contract in contracts_data:
        for service, discount in contract['service_levels'].items():
            if service not in service_level_stats:
                service_level_stats[service] = {
                    'best': {'discount': -float('inf'), 'contract': None},
                    'worst': {'discount': float('inf'), 'contract': None}
                }
            
            if discount > service_level_stats[service]['best']['discount']:
                service_level_stats[service]['best'] = {
                    'discount': discount,
                    'contract': contract['filename']
                }
            
            if discount < service_level_stats[service]['worst']['discount']:
                service_level_stats[service]['worst'] = {
                    'discount': discount,
                    'contract': contract['filename']
                }
    
    sorted_services = sorted(
        service_level_stats.items(),
        key=lambda x: x[1]['best']['discount'],
        reverse=True
    )[:top_n]

    results = {
        'range': f"({format_spend(lower_spend)}-{format_spend(upper_spend)})",
        'confidence': f"Number of contracts in this range: {len(contracts_data)}",
        'top_services': {}
    }
    
    for service, stats in sorted_services:
        results['top_services'][service] = {
            'best': f"{stats['best']['discount']:.3%} at {stats['best']['contract']}",
            'worst': f"{stats['worst']['discount']:.3%} at {stats['worst']['contract']}"
        }
    
    return results

@app.post("/search_contracts/")
async def search_contracts_endpoint(request: SearchRequest):
    """Endpoint for searching contracts based on the request parameters."""
    try:
        results = search_contracts(
            request.target_spend,
            request.carrier,
            request.tolerance,
            request.top_n
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# To run the app, use the following command in the terminal:
# uvicorn app:app --host 0.0.0.0 --port $PORT --reload
