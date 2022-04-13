# Running the Application Locally 


export FLASK_APP=main

export FLASK_ENV=development

flask run


# Docker Commands 

docker image build -t db_web_app . 

docker run -p 5000:5000 -d db_web_app


localhost:5000