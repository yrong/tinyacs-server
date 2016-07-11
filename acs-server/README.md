1. Introduction
SXA Task Management Web Service that is built on top of Vert.x.

2. How to build
Just go to "task-mgmt/java" directory and run "mvn clean install"

3. How to start the service
Run "vertx runmod vertx2~task-mgmt-ws-vertice~0.0.1.DEV-SNAPSHOT" from command line.

The server will be listening on port 8090.

    Pre-condition: Vert.x must have already been installed in the system.

4. How to send HTTP/REST request to the server
4.1 Create a task
 HTTP Method: POST
 URL: http://[server-hostname]:8090
 Payload:   A JSON string that may look like this:
 {
     "queue": "cc_tasks_acs_demo",
     "type": "DbBackup",
     "args": {
             "producerHost": "demo-client",
             "producerApp": "demo",
             "orgId": 20,
             "sn": 10,
             "collectionName": "devices",
             "destination": "/home/ronyang/tmp",
             "delay": 50
     }
 }
 Response: a plain text string that contains the task ID if the task has been successfully en-queued.

4.2 How to retrieve tasks
 HTTP Method: GET
 URL: http://[server-hostname]:8090?[various query parameters]
 Payload:   None
 Available query parameters:
            - orgId
            - id   (task id)
            - queueName
            - username
            - taskType
            - producerHost
            - producerApp
            - state (pending/succeeded/failed, default to any state)
            - brief (true/false, default to false)