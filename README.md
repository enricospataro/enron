# Processing Enron email dataset with Spark

Assumptions:
  - Only the first XML file found in an unzipped directory is processed.
  - Only folder text_000 with text files (emails' bodies) within the unzipped directory is processed.
  - The directory /enron_extract is used to extract a zip file.
  - Only the edrm-enron-v2 folder is processed
  - There are 3 types of email addresses in fields To and CC: LDAP addresses, normal email addresses and full contact names:
      LDAP address is processed with the pattern [RECIPIENTS/CN=]recipient[≶]
      Normal email addresses are processed with the pattern []@[]
      Full contact names - e.g. Surname, Name - are not processe
  - It is assumed that the email bodies are formatted in a proper way, so it is accurate to split on the white space character in order to perform the count of the words
  
Deployment instruction: 
- provision AWS Ubuntu Server 14.04 (t2.large) 
- add the Enron snapshot as a storage volume 250Gb 
- mount enron volume into enron folder in root ('sudo mkdir enron' and then 'sudo mount /dev/xvdb enron') 
- install Scala into /home/ubuntu/scala and Spark into /home/ubuntu/spark 
- copy test.scala to /home/ubuntu and run it to start a full processing

