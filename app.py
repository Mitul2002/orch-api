from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import os
import re
from typing import Dict, List, Tuple
from statistics import mean

app = FastAPI()

# Set base path as constant
BASE_PATH = "clean/"

class SearchRequest(BaseModel):
    target_spend: float
    carrier: str
    tolerance: float

def parse_spend(spend_str: str) -> float:
    """Convert spend string (like $670K or $2.2M) to float value"""
    spend_str = spend_str.replace('$', '').replace(',', '')
    if 'M' in spend_str:
        return float(spend_str.replace('M', '')) * 1_000_000
    elif 'K' in spend_str:
        return float(spend_str.replace('K', '')) * 1_000
    return float(spend_str)

def normalize_discount(discount: float) -> float:
    """Normalize discount value by dividing by 100 if it's greater than 100"""
    if discount > 100:
        return discount / 100
    return discount

def analyze_contracts(
    target_spend: float,
    carrier: str,
    tolerance: float,
) -> Dict:
    """
    Analyze contracts to get statistics for each service level.
    
    Args:
        target_spend: Target spend amount in absolute value
        carrier: 'UPS' or 'FedEx'
        tolerance: Tolerance range as decimal (e.g., 0.2 for 20%)
        
    Returns:
        Dictionary containing service level statistics
    """
    lower_spend = target_spend * (1 - tolerance)
    upper_spend = target_spend * (1 + tolerance)
    carrier_path = os.path.join(BASE_PATH, carrier)
    
    # Dictionary to store discounts by service level
    service_discounts = {}
    
    # Read all contract files in the carrier directory
    for filename in os.listdir(carrier_path):
        if filename.endswith('.csv'):
            spend_match = re.search(r'\$(.+?)\.csv', filename)
            if spend_match:
                contract_spend = parse_spend(spend_match.group(1))
                
                # Check if contract is within spend range
                if lower_spend <= contract_spend <= upper_spend:
                    df = pd.read_csv(os.path.join(carrier_path, filename))
                    current_col = f'CURRENT {carrier.upper()}'
                    
                    for _, row in df.iterrows():
                        service = row['DOMESTIC AIR SERVICE LEVEL']
                        try:
                            discount = normalize_discount(float(row[current_col]))
                            
                            if service not in service_discounts:
                                service_discounts[service] = []
                            service_discounts[service].append(discount)
                        except (ValueError, TypeError):
                            continue  # Skip invalid values
    
    # Calculate statistics for each service level
    service_stats = {}
    for service, discounts in service_discounts.items():
        if discounts:  # Only process if we have valid discounts
            service_stats[service] = {
                'avg_discount': mean(discounts),
                'min_discount': min(discounts),
                'max_discount': max(discounts),
                'contract_count': len(discounts),
                'discount_values': sorted(discounts)
            }
    
    return service_stats

@app.post("/analyze_contracts/")
async def analyze_contracts_endpoint(request: SearchRequest):
    try:
        results = analyze_contracts(
            request.target_spend,
            request.carrier,
            request.tolerance
        )
        return format_results(results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def format_results(stats: Dict) -> str:
    """Format results dictionary into the requested output string"""
    output = []
    
    # Sort services by average discount for consistent output
    sorted_services = sorted(
        stats.items(),
        key=lambda x: x[1]['avg_discount'],
        reverse=True
    )
    
    for service, data in sorted_services:
        service_output = [
            f"\nService Level: {service}",
            f"Average Discount: {data['avg_discount']:.3f}%",
            f"Min Discount: {data['min_discount']:.3f}%",
            f"Max Discount: {data['max_discount']:.3f}%",
            f"Contract Count: {data['contract_count']}",
            f"Discount Values: {', '.join(f'{d:.3f}%' for d in data['discount_values'])}"
        ]
        output.extend(service_output)
    
    return "\n".join(output)

# Run with: uvicorn filename:app --reload
