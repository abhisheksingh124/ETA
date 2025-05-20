import boto3
import json

# Initialize DynamoDB resource
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    try:
        # Log the entire event for debugging
        print(f"Event received: {json.dumps(event)}")
        
        # Extract user input more safely with error handling
        try:
            # Handle different possible event structures
            user_input_data = None
            
            # Check if this is coming from a Bedrock Agent
            if 'requestBody' in event and 'content' in event['requestBody']:
                if 'application/json' in event['requestBody']['content']:
                    properties = event['requestBody']['content']['application/json'].get('properties', [])
                    if properties and len(properties) > 0:
                        user_input_data = properties[0].get('value')
            
            # Alternative path if the event structure is different
            elif 'body' in event:
                body_content = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
                user_input_data = body_content.get('empID')
            
            # If we still don't have the employee ID, check if it's directly in the event
            if user_input_data is None and 'empID' in event:
                user_input_data = event['empID']
                
            # Convert to string if it's not already
            if user_input_data is not None:
                user_input_data = str(user_input_data)
            
            if not user_input_data:
                raise ValueError("Employee ID not found in the request")
                
            print(f"The employee id is {user_input_data}")
            
        except Exception as e:
            print(f"Error extracting employee ID: {str(e)}")
            raise
        
        # Query DynamoDB with detailed error handling
        try:
            # First, validate if empID format is correct (should be numeric)
            if not user_input_data.isdigit():
                print(f"Invalid employee ID format: {user_input_data}. Must be numeric.")
                return format_response(
                    event,
                    {"error": f"Invalid employee ID format: {user_input_data}. Employee ID must be numeric."},
                    400
                )
            
            # Print table info for debugging
            try:
                table_info = client.describe_table(TableName='leaveBalanceHRTable')
                print(f"Table exists with info: {json.dumps(table_info, default=str)}")
            except Exception as table_error:
                print(f"Could not describe table: {str(table_error)}")
            
            print(f"Attempting to query DynamoDB for employee ID: {user_input_data}")
            
            # Check if we can scan the table
            try:
                scan_result = client.scan(
                    TableName='leaveBalanceHRTable',
                    Limit=5  # Just get a few items to confirm connectivity
                )
                print(f"Scan sample results: {json.dumps(scan_result, default=str)}")
            except Exception as scan_error:
                print(f"Scan error (informational only): {str(scan_error)}")
            
            # Now try the actual get_item
            response = client.get_item(
                TableName='leaveBalanceHRTable', 
                Key={'empID': {'N': user_input_data}}
            )
            
            print(f"DynamoDB response: {json.dumps(response, default=str)}")
            
            # Check if the item exists
            if 'Item' not in response:
                print(f"Employee with ID {user_input_data} not found in database")
                return format_response(
                    event,
                    {"error": f"Employee with ID {user_input_data} not found in the leave balance database"},
                    404
                )
            
            print(f"Successfully retrieved leave balance for employee {user_input_data}")
            leave_balance = response['Item']
            
        except client.exceptions.ResourceNotFoundException:
            print(f"Table leaveBalanceHRTable not found")
            return format_response(
                event,
                {"error": "DynamoDB table leaveBalanceHRTable not found"},
                404
            )
        except client.exceptions.ProvisionedThroughputExceededException:
            print(f"DynamoDB throughput exceeded for employee ID: {user_input_data}")
            return format_response(
                event,
                {"error": "Database is currently experiencing high traffic. Please try again later."},
                429
            )
        except client.exceptions.AccessDeniedException as access_error:
            print(f"Access denied to DynamoDB: {str(access_error)}")
            return format_response(
                event,
                {"error": "The function does not have permission to access the leave balance database"},
                403
            )
        except Exception as e:
            print(f"Error querying DynamoDB for employee ID {user_input_data}: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            print(f"Error details: {str(e)}")
            return format_response(
                event,
                {"error": f"Failed to retrieve employee data: {str(e)}"},
                500
            )
        
        # Return formatted response with human-readable data
        if leave_balance:
            # Convert DynamoDB format to regular JSON
            readable_leave_balance = {}
            for key, value_dict in leave_balance.items():
                # Extract the value from DynamoDB type dictionary
                # e.g., {'N': '10'} becomes 10, {'S': 'John'} becomes 'John'
                value_type = list(value_dict.keys())[0]  # Get 'N', 'S', etc.
                raw_value = value_dict[value_type]
                
                # Convert numeric values to appropriate type
                if value_type == 'N':
                    try:
                        # Try to convert to int if it's a whole number
                        if '.' not in raw_value:
                            readable_leave_balance[key] = int(raw_value)
                        else:
                            readable_leave_balance[key] = float(raw_value)
                    except ValueError:
                        readable_leave_balance[key] = raw_value
                else:
                    readable_leave_balance[key] = raw_value
            
            print(f"Returning readable leave balance: {json.dumps(readable_leave_balance)}")
            return format_response(event, readable_leave_balance, 200)
        
        # If we somehow got here without leave_balance, return an error
        return format_response(
            event,
            {"error": "An unexpected error occurred while processing the leave balance data"},
            500
        )
        
    except Exception as e:
        print(f"Unhandled exception: {str(e)}")
        return format_response(
            event,
            {"error": f"Internal server error: {str(e)}"},
            500
        )

def format_response(event, body_content, status_code=200):
    """Helper function to format response according to Bedrock Agent requirements"""
    
    response_body = {
        'application/json': {
            'body': json.dumps(body_content)
        }
    }
    
    print(f"Response Body: {response_body}")
    
    # Format for Bedrock Agent
    if 'agent' in event and 'actionGroup' in event:
        action_response = {
            'actionGroup': event.get('actionGroup', ''),
            'apiPath': event.get('apiPath', ''),
            'httpMethod': event.get('httpMethod', 'GET'),
            'httpStatusCode': status_code,
            'responseBody': response_body
        }
        
        api_response = {
            'messageVersion': '1.0',
            'response': action_response,
            'sessionAttributes': event.get('sessionAttributes', {}),
            'promptSessionAttributes': event.get('promptSessionAttributes', {})
        }
        
        return api_response
    
    # Standard API Gateway response format for direct invocation
    else:
        return {
            'statusCode': status_code,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(body_content)
        }
