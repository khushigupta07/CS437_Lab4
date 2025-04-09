import boto3

iot = boto3.client('iot', region_name='us-east-2')

THING_GROUP_NAME = 'thing_group1'
POLICY_NAME = 'policy1'
NUM_DEVICES = 5  # Change to 1000+ later if needed

for i in range(NUM_DEVICES):
    thing_name = f"Vehicle_{i}"

    # Step 1: Create Thing
    thing = iot.create_thing(thingName=thing_name)
    print(f"Created Thing: {thing_name}")

    # Step 2: Create cert + keys
    cert = iot.create_keys_and_certificate(setAsActive=True)
    cert_arn = cert['certificateArn']
    cert_id = cert['certificateId']

    # Step 3: Attach policy
    iot.attach_policy(policyName=POLICY_NAME, target=cert_arn)

    # Step 4: Attach cert to thing
    iot.attach_thing_principal(
        thingName=thing_name,
        principal=cert_arn
    )

    # Step 5: Add to Thing Group
    iot.add_thing_to_thing_group(
        thingGroupName=THING_GROUP_NAME,
        thingName=thing_name
    )

    # (Optional) Save certs/keys locally
    with open(f"certs/{thing_name}_certificate.pem.crt", "w") as f:
        f.write(cert['certificatePem'])
    with open(f"certs/{thing_name}_private.pem.key", "w") as f:
        f.write(cert['keyPair']['PrivateKey'])
    with open(f"certs/{thing_name}_public.pem.key", "w") as f:
        f.write(cert['keyPair']['PublicKey'])

print("Done.")
