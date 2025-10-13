#!/usr/bin/env -S python3 -u

# This file serves as a dedicated test driver for etcd's lease functionality.
# It creates a number of keys with a short-term lease (TTL), waits for the
# lease to expire, and then asserts that etcd has correctly deleted the keys.
# This is a critical test for time-based state management and distributed garbage collection.

import time
import sys
sys.path.append("/opt/antithesis/resources")
import helper

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
)


def create_leased_keys(lease_ttl_seconds):
    """
    Connects to an etcd host and creates a number of keys, each with an TTL.
    """

    num_keys_to_create = helper.generate_requests()
    print(f"Client: Attempting to create {num_keys_to_create} keys with a {lease_ttl_seconds}s lease.")
    client = helper.connect_to_host()

    leased_kvs = []

    for _ in range(num_keys_to_create):
        key = helper.generate_random_string()
        value = helper.generate_random_string()

        # Use the etcd3 client to grant a lease.
        lease = client.lease(lease_ttl_seconds)

        # Put the key and associate it with the created lease.
        success, error = helper.put_request_lease(client, key, value, lease)

        # Record the key and when we expect it to expire for later validation.
        sometimes(success, "Client can make successful put requests", {"error":error})

        if success:
            expiration_time = time.time() + lease.ttl
            leased_kvs.append({'key': key, 'expires_at': expiration_time})
            print(f"Client: successful put with key '{key}' and expiration time '{expiration_time}'")
        else:
            print(f"Client: unsuccessful put with key '{key}', value '{value}', and error '{error}'")


    return leased_kvs


def validate_lease_expiration(leased_kvs):
    """
    Validates that keys created with a lease are deleted after the lease expires.
    """
    if not leased_kvs:
        print("Client: No leased keys were created, skipping validation.")
        return

    # Find the latest expiration time among all the keys we created.
    latest_expiry = max(item['expires_at'] for item in leased_kvs)

    # Add a 2-second buffer to account for potential clock skew or network delays.
    wait_until = latest_expiry + 2.0
    sleep_duration = max(0, wait_until - time.time())

    print(f"Client: Waiting for {sleep_duration:.2f} seconds for all leases to expire...")
    time.sleep(sleep_duration)

    client = helper.connect_to_host()
    print("Client: Validating lease expiration for all created keys...")

    for item in leased_kvs:
        key = item['key']

        # If the key does not exist, metadata will be None.
        _, error, metadata = helper.get_request(client, key)
        key_was_found = metadata is not None

        # The condition `key_was_found` MUST ALWAYS be false at this point.
        # If it is ever true, it means etcd failed to delete a key after its
        # lease expired, which is a serious bug. Antithesis will find and report this.
        always(
            not key_was_found,
            "Leased key must be deleted after expiration",
            {
                "key": key,
                "expected_expiry_timestamp": item['expires_at'],
                "check_timestamp": time.time()
            }
        )

        if key_was_found:
            print(f"Client: VALIDATION FAILED! Leased key '{key}' still exists after it should have expired.")
        else:
            print(f"Client: Validation OK. Expired key '{key}' was correctly deleted.")


if __name__ == "__main__":
    LEASE_TTL_SECONDS = 5

    # Create the keys with a lease.
    successfully_created_leases = create_leased_keys(LEASE_TTL_SECONDS)

    # Wait for the lease duration and validate that the keys are gone.
    validate_lease_expiration(successfully_created_leases)
