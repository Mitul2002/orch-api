from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import os
import re
from typing import Dict, List, Tuple

app = FastAPI()

class SearchRequest(BaseModel):
    base_path: str
    target_spend: float
    carrier: str
    tolerance: float
    top_n: int

def parse_spend(spend_str: str) -> float:
    spend_str = spend_str.replace('$', '').replace(',', '')
    if 'M' in spend_str:
        return float(spend_str.replace('M', '')) * 1_000_000
    elif 'K' in spend_str:
        return float(spend_str.replace('K', '')) * 1_000
    return float(spend_str)

def format_spend(spend: float) -> str:
    if spend >= 1_000_000:
        return f"${spend / 1_000_000:.1f}M"
    return f"${spend / 1_000:.0f}K"

def get_spend_range(spend: float, tolerance: float) -> Tuple[float, float]:
    lower = spend * (1 - tolerance)
    upper = spend * (1 + tolerance)
    return lower, upper

def normalize_discount(discount: float) -> float:
    return discount / 100 if discount > 100 else discount

def search_contracts(
    base_path: str,
    target_spend: float,
    carrier: str,
    tolerance: float,
    top_n: int
) -> Dict:
    lower_spend, upper_spend = get_spend_range(target_spend, tolerance)
    carrier_path = os.path.join(base_path, carrier)
    
    contracts_data = []
    
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
        'confidence': f"no of contracts in this range: {len(contracts_data)}",
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
    try:
        results = search_contracts(
            request.base_path,
            request.target_spend,
            request.carrier,
            request.tolerance,
            request.top_n
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# To run the app, use the following command in the terminal:
# uvicorn script_name:app --reload
