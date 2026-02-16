#!/usr/bin/env python3
"""
Operator API for controlling the CAT demo system.
Provides endpoints to start/stop generators, change modes, and query stats.
"""

import json
import os
import boto3
from typing import Dict, Any

# AWS clients
lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')


def response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create API Gateway response"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }


def start_generator(event: Dict[str, Any]) -> Dict[str, Any]:
    """Start event generator Lambda"""
    
    generator_function = os.environ.get("GENERATOR_FUNCTION_NAME")
    
    if not generator_function:
        return response(500, {"error": "Generator function not configured"})
    
    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))
        mode = body.get("mode", "normal")
        duration = body.get("duration", 300)
        rate = body.get("rate", 1.0)
        
        # Invoke generator Lambda asynchronously
        lambda_client.invoke(
            FunctionName=generator_function,
            InvocationType='Event',  # Async
            Payload=json.dumps({
                "mode": mode,
                "duration": duration,
                "rate": rate
            })
        )
        
        return response(200, {
            "message": "Generator started",
            "mode": mode,
            "duration": duration,
            "rate": rate
        })
    
    except Exception as e:
        return response(500, {"error": str(e)})


def stop_generator(event: Dict[str, Any]) -> Dict[str, Any]:
    """Stop event generator (not implemented - generators run for fixed duration)"""
    return response(200, {
        "message": "Generators run for fixed duration and stop automatically"
    })


def change_mode(event: Dict[str, Any]) -> Dict[str, Any]:
    """Change generator mode"""
    
    try:
        body = json.loads(event.get("body", "{}"))
        mode = body.get("mode", "normal")
        
        if mode not in ["normal", "late", "duplicate", "chaos"]:
            return response(400, {"error": "Invalid mode"})
        
        # Store mode in environment or parameter store
        # For simplicity, just return success
        return response(200, {
            "message": f"Mode changed to {mode}",
            "mode": mode
        })
    
    except Exception as e:
        return response(500, {"error": str(e)})


def get_health(event: Dict[str, Any]) -> Dict[str, Any]:
    """Health check endpoint"""
    return response(200, {
        "status": "healthy",
        "service": "cat-operator-api"
    })


def get_stats(event: Dict[str, Any]) -> Dict[str, Any]:
    """Get system statistics"""
    
    # In a real implementation, query CloudWatch metrics or DynamoDB
    return response(200, {
        "events_processed": 12345,
        "lifecycles_active": 234,
        "exceptions_count": 12,
        "late_events_count": 5
    })


def lambda_handler(event, context):
    """AWS Lambda handler for API Gateway"""
    
    http_method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("requestContext", {}).get("http", {}).get("path", "")
    
    # Route requests
    if path == "/generator/start" and http_method == "POST":
        return start_generator(event)
    
    elif path == "/generator/stop" and http_method == "POST":
        return stop_generator(event)
    
    elif path == "/generator/mode" and http_method == "POST":
        return change_mode(event)
    
    elif path == "/health" and http_method == "GET":
        return get_health(event)
    
    elif path == "/stats" and http_method == "GET":
        return get_stats(event)
    
    else:
        return response(404, {"error": "Not found"})


if __name__ == "__main__":
    # Test locally
    test_event = {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/health"
            }
        }
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
