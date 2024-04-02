import os
import json
import boto3
import urllib.request


# Expected environment variables
CREDAL_API_KEY = os.environ["CREDAL_API_KEY"]
CREDAL_API_EMAIL = os.environ["CREDAL_API_EMAIL"]

INPUT_BUCKET_NAME = "<input-bucket-name>"
OUTPUT_BUCKET_NAME = "<output-bucket-name>"
# Not specifying a prefix. For now, paste everything in the root directory
# of the bucket.


def chunk_string(input_string, chunk_size) -> list[str]:
    return [input_string[i:i + chunk_size] for i in range(0, len(input_string), chunk_size)]


def read_ai_prompt_from_bucket() -> str:
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=INPUT_BUCKET_NAME, Key="ai_prompts/ai_prompt.txt")
    return response["Body"].read().decode("utf-8")


def get_document_text_content(job_id: str) -> str:
    textract = boto3.client("textract")

    text_content = ""
    response = textract.get_document_text_detection(JobId=job_id)
    text_content += "\n".join(
        [block["Text"] for block in response["Blocks"] if block["BlockType"] == "LINE"]
    )

    # Get more content if exists
    next_token = response.get("NextToken")
    while next_token:
        response = textract.get_document_text_detection(
            JobId=job_id, NextToken=next_token
        )
        text_content += "\n".join(
            [block["Text"] for block in response["Blocks"] if block["BlockType"] == "LINE"]
        )

        next_token = response.get("NextToken")

    return text_content


def extract_patient_data_using_credal_single_chunk(patient_record_text: str) -> dict:
    message_to_send = read_ai_prompt_from_bucket()
    message_to_send += "\n\n# Patient Medical Record Text Content"

    # Because using the prompt with single chunk
    message_to_send += "\n" + patient_record_text.replace(
        "(which I've shown you in previous messages 'Patient Medical Record Text Content' heading)",
        ""
    )

    request_data = {
        "message": message_to_send,
        "userEmail": CREDAL_API_EMAIL,
        "origin": "Web",
    }
    req = urllib.request.Request(
        url="https://api.credal.ai/api/v0/copilots/sendMessage",
        data=json.dumps(request_data).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CREDAL_API_KEY}",
            "Accept": "application/json"
        },
        method="POST"
    )

    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode("utf-8"))
        print("- Response:", res)

    reply = res["sendChatResult"]["response"]["message"]
    
    # Just in case he's being silly
    if "```" in reply:
        reply = reply[reply.index("```") + 3:].replace("```", "").strip()
    
    # Remove any number of characters until we encounter '{' which indicates
    # start of JSON text
    start_index = 0
    for i in range(20):
        # 20 is just a guess so I don't run an infinite loop
        if reply[i] == "{":
            start_index = i
            break
    
    reply = reply[start_index:]

    return json.loads(reply, strict=False)


def extract_patient_data_using_credal(patient_record_text: str) -> dict:
    ai_prompt = read_ai_prompt_from_bucket()

    chunk_size = 70000
    print("Length of patient record", len(patient_record_text))
    print(f"Using chunk size of {chunk_size}")

    chunks = chunk_string(input_string=patient_record_text, chunk_size=chunk_size)
    print("Total chunks", len(chunks))

    if len(chunks) == 1:
        return extract_patient_data_using_credal_single_chunk(patient_record_text)

    conversation_id = None
    for i in range(len(chunks)):
        print(f"--- REQUESTING WITH CHUNK # {i} ---")
        message_to_send = ""

        if i == 0:
            message_to_send += """
                I want to give you text content of a patient record's PDF. The content is large,
                so I'm going to give it to you in multiple chunks, so keep waiting for more
                messages from me, unless I give you instructions on how you should respond.
                Just say 'waiting for more content' in reply, unless I tell you that the patient
                medical record is complete.
            """

        message_to_send += chunks[i] + "\n\n"

        # Include a keyword at the end to specify whether the patient record has ended or not
        if i == len(chunks) - 1:
            message_to_send += f"""
            # PATIENT MEDICAL RECORD COMPLETE!
            Follow these instructions now;
            """
            message_to_send += ai_prompt
        else:
            message_to_send += "Patient record to be continued..."

        request_data = {
            "message": message_to_send,
            "userEmail": CREDAL_API_EMAIL,
            "origin": "Web",
        }
        if conversation_id:
            # If conversation ID exists, it means a first message was already sent
            request_data["conversationId"] = conversation_id

        req = urllib.request.Request(
            url="https://api.credal.ai/api/v0/copilots/sendMessage",
            data=json.dumps(request_data).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {CREDAL_API_KEY}",
                "Accept": "application/json"
            },
            method="POST"
        )

        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode("utf-8"))
            print("- Response:", res)

            # Now, also include the conversation ID in request data.
            conversation_id = res["sendChatResult"]["conversationId"]

    print("All chunks sent!")

    # We're expecting the final reply to be a JSON with the result
    reply = res["sendChatResult"]["response"]["message"]
    
    # Just in case he's being silly
    if "```" in reply:
        reply = reply[reply.index("```") + 3:].replace("```", "").strip()
    
    # Remove any number of characters until we encounter '{' which indicates
    # start of JSON text
    start_index = 0
    for i in range(20):
        # 20 is just a guess so I don't run an infinite loop
        if reply[i] == "{":
            start_index = i
            break
    
    reply = reply[start_index:]

    return json.loads(reply, strict=False)


def lambda_handler(event, context):
    # TODO: See if we have to handle multiple records
    print("Number of records in the event", len(event["Records"]))

    message = json.loads(event["Records"][0]["Sns"]["Message"])
    job_id = message["JobId"]
    
    # We want the output file to be named according to what we expect
    input_file_name = message["DocumentLocation"]["S3ObjectName"]
    output_file_name = "output_" + input_file_name.replace(".pdf", "") + ".json"

    text_content = get_document_text_content(job_id=job_id)
    
    patient_data = extract_patient_data_using_credal(
        patient_record_text=text_content
    )

    # Save output to bucket
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=OUTPUT_BUCKET_NAME,
        Key=output_file_name,
        Body=json.dumps(patient_data, sort_keys=True, indent=4),
    )

    return {"statusCode": 200, "body": json.dumps("File uploaded successfully!")}
