#1 imports 
import boto3
import json

#2 Initialize DynamoDB resource/Create a client connection- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
client = boto3.client('dynamodb')

def lambda_handler(event, context):
#3 Store the user input 
    print(f"The user input is {event}")
    user_input_data = event['requestBody']['content']['application/json']['properties'][0]['value']
    print(f"The employee id is {user_input_data}")
    
#4 Reference the dynamodb table and retrieve data - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/get_item.html
    response = client.get_item (TableName='leaveBalanceHRTable', Key={'empID': {'N': user_input_data}})
    print(f"The leave balance is {response}")
    leave_balance = response['Item']
    #return leave_balance
    
#5 Format the response as per the requirement of Bedrock Agent Action Group - https://docs.aws.amazon.com/bedrock/latest/userguide/agents-lambda.html
    agent = event['agent']
    actionGroup = event['actionGroup']
    api_path = event['apiPath']
#6 Use get parameters as data retrieval
    get_parameters = event.get('parameters', [])
#7 Replace the value of 'body' and print the value of Response Body
    response_body = {
        'application/json': {
            'body': json.dumps(leave_balance)
        }
    }
    
    print(f"Response Body is {response_body}")
    
#8 Add rest of code as expected by Agent - https://docs.aws.amazon.com/bedrock/latest/userguide/agents-lambda.html
    action_response = {
        'actionGroup': event['actionGroup'],
        'apiPath': event['apiPath'],
        'httpMethod': event['httpMethod'],
        'httpStatusCode': 200,
        'responseBody': response_body
    }
    
    session_attributes = event['sessionAttributes']
    prompt_session_attributes = event['promptSessionAttributes']
    
    api_response = {
        'messageVersion': '1.0', 
        'response': action_response,
        'sessionAttributes': session_attributes,
        'promptSessionAttributes': prompt_session_attributes
    }
    
    print (api_response)    
    return api_response