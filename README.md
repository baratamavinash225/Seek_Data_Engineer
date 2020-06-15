Restaurant ChatBot:
‘Foodie’ restaurant chatbot has been created using open source chat framework RASA. This integrates with Zomato API to fetch restaurant information of the Tier1 and Tier2 cities with the limited cuisine and the price range.

Installation Pre-requisites:

 - python 3.7.3
 - rasa 1.10.1
 - spacy 2.2.2
 - en_core_web_md 2.2.0

Additional Libraries used/added:
 - pandas
 - smtplib

Installation Steps:

 - Refer to official installation guide to install RASA as well as the additional requisites for the spacy
 - pandas
   `pip install pandas`

Repository Information:

This repo contains training data and script files necessary to compile and execute this restaurant chatbot. It comprises of the following files:

  File Details:
   - data/nlu/nlu.md : contains examples, synonymns and various statements of the intents for the NLU to understand.
   - data/core/stories.md : contains the training stories of the bot.
   - actions.py : contains the following custom actions which are necessary for the bot to connect to Zomato API, fetch and return results to bot. Also it has the custom actions for the validation of the cuisine, price, email etc.,
   - cities.json : contains all the Tier 1 and Tier 2 cities listed for the bot to validate.
   - config.yml: contains model configuration and custom policy - used `pretrained_embeddings_spacy` policy.
   - credentials.yml: contains authentication token to connect with channels like slack
   - domain.yml: defines chatbot domain like entities, actions, templates, slots
   - endpoints.yml: contains the webhook configuration for custom action

Modifications done to the base:
- actions.py:
   - ActionSearchRestaurants - This action search the restaurants based on the location, cuisine, price and return the result of the top 5 restaurants based on the user ratings.
     - This calls the API and fetch the results, then filter records on the price range entered by the user and sort it with user ratings and return 5 records.
     - Zomato API key should be added `Line 26` to connect to Zomato API and fetch results.
   - ActionSendEmailRestaurentDetails - When the user requests to send email with the list of 10 restaurants
      - It validates the user email id - if success it emails the top 10 restaurant details based on the zomato user rating(returned in API) and send email to the user with the details. 
      - If the email id is invalid, then it returns an utter_action to enter correct email ID.
      - This uses the google API to send email to the respective mail id.
      - Zomato API key should be added at `line 84` to connect to Zomato API and fetch results.
      - Email details should be added in `Line 208` so that this functionality will work.
  - ActionValidateLocation -  This takes the location input from the user entered, it validates against the cities.json which has Tier1 and Tier 2 cities.
    - If the location is valid then, it sets a slot and returns to ask for the price/cuisine which is not asked.
    - If the location is invalid, then it returns an utter_action to enter correct Location from Tier 1 and Tier 2 cities.
   - ActionValidateCuisine - This takes the cuisine input from the user entered, it validates against the 6 cuisines which are supported by bot.
     - If the cuisine is valid then, it sets a slot and returns to ask for the price which is not asked.
     - If the cuisine is invalid, then it returns an utter_action to select the cuisine only from the 6 options.
   - ActionValidatePrice - This takes the price input from the user entered, it validates against the 3 range of prices which are supported by bot.
     - If the price range is valid then, it sets a slot and returns back with the list of restaurants.
     - If the price range is invalid, then it returns an utter_action to select the price range only from the 3 options.
   - ActionChitchat - This is used whenever the user is discussing about out of topic conversations. We have defined few in general questions which can be asked by user and created an action, whenever such questions are asked bot will redirect with the utter respective response.
     - ask_builder - Asking about who has built it.
     - ask_howdoing - Asking about how are you doing
     - ask_howold - Asking about how old are you
     - out_of_scope - Asking about the Whether or other details
     - bot_challenge - Asking about what the bot does.
     - handle_insult - Asking about insulting questions(will you marry me etc.)
   - ActionResetSlots -  This Action is to reset the slot values incase if the conversation is totally completed so that user can start fresh.
- domain.yml:
  - Actions - Added the actions mentioned above to handle the various scenarios necessary for the bot to operate.
  - Entities - Added price, cuisine, location and email_id as new entities.
  - intents - Added send_email_with_restaurant_details, no_email, chit chat intents and few more intents to handle.
  - responses - Added new buttons for the cuisine and price validity, respective chit chat responses and the respective validation(price, email_id, cuisine and location) responses.
  - slots - Added the slots related to price, cuisine, location, email id and the respective validation slots. 
- data/nlu.md: Added many more example intents so that rasa nlu can recognise the respective slot values and the intents
  - intents - Chitchat intents, no_email, out_of_scope, send_email_with_restaurant_details and restaurant_search etc with further more examples so that bot can understand that it belongs to respective intent.
  - synonym - Added multiple synonyms to hand the locations(Ex: Delhi can be called as Dilli etc), cuisine, price.
- data/stories.md  - Trained and created(50 stories) so that the bot with the multiple stories so that rasa can recognise the probabilities and redirect to the respective actions very well.

Usages
 - Train RASA NLU & CORE using below rasa cli command :
`rasa train`
 This will generate <yyyymmdd-hhmmss>.tar.gz inside models folder .
 - Run RASA action server using below RASA CLI command:
`rasa run actions`
 - Start an interactive session with restaurant chatbot using RASA CLI command
`rasa shell`

