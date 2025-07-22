-- ============================================================================
-- Snowflake Streams and Tasks Implementation - SNS Notification Integration
-- ============================================================================
-- Purpose: Creates Amazon SNS notification integration for task error alerts
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. CREATE SNS NOTIFICATION INTEGRATION
-- ============================================================================

-- Create notification integration for Amazon SNS
-- Note: Replace the AWS_SNS_TOPIC_ARN and AWS_SNS_ROLE_ARN with your actual values
CREATE OR REPLACE NOTIFICATION INTEGRATION STREAMS_TASKS_SNS_INTEGRATION
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AWS_SNS
    DIRECTION = OUTBOUND
    AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:snowflake-task-errors'
    AWS_SNS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeNotificationRole'
    ENABLED = TRUE
    COMMENT = 'SNS integration for Snowflake task error notifications';

-- ============================================================================
-- 2. GRANT PRIVILEGES ON NOTIFICATION INTEGRATION
-- ============================================================================

-- Grant usage on the notification integration to the streams tasks role
GRANT USAGE ON INTEGRATION STREAMS_TASKS_SNS_INTEGRATION TO ROLE STREAMS_TASKS_ROLE;

-- ============================================================================
-- 3. VERIFY NOTIFICATION INTEGRATION
-- ============================================================================

-- Show notification integrations
SHOW INTEGRATIONS LIKE 'STREAMS_TASKS_SNS_INTEGRATION';

-- Describe the integration to get details
DESC NOTIFICATION INTEGRATION STREAMS_TASKS_SNS_INTEGRATION;

-- ============================================================================
-- 4. TEST NOTIFICATION INTEGRATION (OPTIONAL)
-- ============================================================================

-- Test the notification integration by sending a test message
-- Note: Uncomment the following to test the integration
/*
CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
    'STREAMS_TASKS_SNS_INTEGRATION',
    'Test notification from Snowflake Streams and Tasks pipeline',
    'This is a test message to verify the SNS integration is working correctly.'
);
*/

-- ============================================================================
-- 5. AWS SETUP INSTRUCTIONS (COMMENTS)
-- ============================================================================

/*
AWS SETUP REQUIREMENTS:

1. CREATE SNS TOPIC:
   - Log into AWS Console
   - Navigate to SNS service
   - Create a new topic (e.g., 'snowflake-task-errors')
   - Note the Topic ARN

2. CREATE IAM ROLE FOR SNOWFLAKE:
   - Create IAM role with the following trust policy:
   
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "arn:aws:iam::334519176434:root"
         },
         "Action": "sts:AssumeRole",
         "Condition": {
           "StringEquals": {
             "sts:ExternalId": "<SNOWFLAKE_EXTERNAL_ID>"
           }
         }
       }
     ]
   }

3. ATTACH POLICY TO ROLE:
   - Create and attach policy with SNS publish permissions:
   
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "sns:Publish"
         ],
         "Resource": "arn:aws:sns:us-east-1:123456789012:snowflake-task-errors"
       }
     ]
   }

4. GET EXTERNAL ID:
   - After creating the integration, run DESC NOTIFICATION INTEGRATION
   - Use the SF_AWS_EXTERNAL_ID in your IAM role trust policy

5. SUBSCRIBE TO SNS TOPIC:
   - Add email, SMS, or other endpoints to receive notifications
   - Confirm subscriptions as needed

6. UPDATE INTEGRATION:
   - Replace placeholder ARNs in this script with actual values
   - Re-run the CREATE NOTIFICATION INTEGRATION command
*/

-- ============================================================================
-- 6. MONITORING QUERIES
-- ============================================================================

-- Query to check notification integration status
SELECT 
    'Notification Integration Status' AS info,
    NAME,
    TYPE,
    CATEGORY,
    ENABLED,
    CREATED_ON,
    COMMENT
FROM INFORMATION_SCHEMA.INTEGRATIONS 
WHERE NAME = 'STREAMS_TASKS_SNS_INTEGRATION';

-- Query to check notification history (if available)
-- Note: This may require specific privileges and may not be available in all accounts
/*
SELECT 
    NOTIFICATION_NAME,
    NOTIFICATION_TYPE,
    MESSAGE,
    CREATED_ON,
    STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.NOTIFICATIONS
WHERE NOTIFICATION_NAME = 'STREAMS_TASKS_SNS_INTEGRATION'
ORDER BY CREATED_ON DESC
LIMIT 10;
*/

SELECT 'SNS Notification Integration created successfully!' AS status;
SELECT 'Remember to update AWS ARNs with actual values and complete AWS setup.' AS reminder;
