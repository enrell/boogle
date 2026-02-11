#!/bin/bash
# Quick End-to-End Test - Run all tests at once
bash -x /home/kokoro/boogle/test_all.sh 2>&1 | tee /tmp/test_all_output.log
