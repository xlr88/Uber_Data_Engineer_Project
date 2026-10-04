 Create event hub namespace with standard tier to get kafa translation layer.

 So create a uber topic in event hub namespace so where we create two shared access policies, those are send and listen ploicies. we will have primary connection and secondary connection string and for this Uber API uvicon app. We use Send policy connection string in the ENV file with the topic name. As of now, I am deleting the event, name space to save cost and will be created later.

 And the listen policy function string will be there in bronze injection.ETL pipeline which is created in data bricks, community edition for free edition.

 

