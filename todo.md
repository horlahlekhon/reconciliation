* Include centralized structured logging
* Convert all the views to use Clas based views
* Add logic for validating datatypes. i.e comprehensive regex that matches different datatype before arbitrarily settling for fields to be text
* Add exception handler and custom consistent response format
* Handle csv without any data rows, i.e only headers
* Implement code formatting (black, isort)
* Add type hint
* Add job expiration/cleanup for old jobs
* Handle Duplicate columns
* Add minimal tests
* Add docker file and docker compose
* Include other features to extend the task :
    * Task cancellation from the API
    * 