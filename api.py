from flask import Flask, jsonify, request
from flask import Response
from werkzeug.exceptions import BadRequestKeyError
from flasgger import Swagger, swag_from
import config
from config import Configuration
import logger
from ecom_g_ads import GAdsEcomru
from ecom_generate_user_credentials import GAdsCredentialsEcomru

import os
import flask
import requests
import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery

app = Flask(__name__)
app.config.from_object(Configuration)
app.config['SWAGGER'] = {"title": "GTCOM-GAdsApi", "uiversion": 3}
app.secret_key = '1234'

logger = logger.init_logger()

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec_1",
            "route": "/apispec_1.json()",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/swagger/",
}

swagger = Swagger(app, config=swagger_config)


class HttpError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


def to_boolean(x):
    if x == "true":
        return True
    elif x == "false":
        return False
    else:
        return None


@app.after_request
def apply_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "content-type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST"
    return response


@app.route('/googleads/auth', methods=['GET'])
@swag_from("swagger_conf/auth.yml")
def auth():
    """Генерирует ссылку, сохраняет токен"""

    try:
        credentials = GAdsCredentialsEcomru(
            client_secrets_path='client_secret_660722585949-478ao09befcevaosv1ot311so9dt5575.apps.googleusercontent.com.json')

        url = credentials.authorization_url

        refresh_token = credentials.generate_refresh_token()

        return jsonify({'token': refresh_token})

    except BadRequestKeyError:
        logger.error("get_campaigns: BadRequest")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'get_campaigns: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/getcampaigns', methods=['POST'])
@swag_from("swagger_conf/get_campaigns.yml")
def get_campaigns():
    """Получить кампании"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        show_removed = json_file.get("show_removed", "false")

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"get_campaigns: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.get_campaigns(customer_id=customer_id, show_removed=to_boolean(show_removed))
            # logger.info(f"")
            return jsonify(result)

    except BadRequestKeyError:
        logger.error("get_campaigns: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("get_campaigns: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'get_campaigns: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/getadgroups', methods=['POST'])
@swag_from("swagger_conf/get_adgroups.yml")
def get_adgroups():
    """Получить группы"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        show_removed = json_file.get("show_removed", "false")
        campaign_id = json_file.get("campaign_id", None)

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"get_adgroups: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.get_ad_groups(customer_id=customer_id,
                                        campaign_id=campaign_id,
                                        show_removed=to_boolean(show_removed))
            return jsonify(result)

    except BadRequestKeyError:
        logger.error("get_adgroups: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("get_adgroups: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'get_adgroups: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/getkeywords', methods=['POST'])
@swag_from("swagger_conf/get_keywords.yml")
def get_keywords():
    """Получить ключевые слова"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        omit_unselected_resource_names = json_file.get("omit_unselected_resource_names", "false")
        ad_group_id = json_file.get("ad_group_id", None)

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"get_keywords: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.get_keywords(customer_id=customer_id,
                                       omit_unselected_resource_names=to_boolean(omit_unselected_resource_names),
                                       ad_group_id=ad_group_id)
            return jsonify(result)

    except BadRequestKeyError:
        logger.error("get_keywords: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("get_keywords: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'get_keywords: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/addkeywordsplan', methods=['POST'])
@swag_from("swagger_conf/add_keywords_plan.yml")
def add_keywords_plan():
    """Добавить план ключевых слов"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        keywords = json_file["keywords"]
        bids = json_file["bids"]
        negative_keywords = json_file.get("negative_keywords", None)

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"add_keywords_plan: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.add_keyword_plan(customer_id=customer_id, keywords=keywords, bids=bids,
                                           negative_keywords=negative_keywords)
            return jsonify(result)

    except BadRequestKeyError:
        logger.error("add_keywords_plan: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("add_keywords_plan: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'add_keywords_plan: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/addcampaign', methods=['POST'])
@swag_from("swagger_conf/add_campaign.yml")
def add_campaign():
    """Добавить кампанию"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        campaign_budget_name = json_file["campaign_budget_name"]
        campaign_budget_amount = json_file["campaign_budget_amount"]
        campaign_name = json_file["campaign_name"]
        date_from = json_file.get("date_from", None)
        date_to = json_file.get("date_to", None)
        enhanced_cpc_enabled = json_file.get("enhanced_cpc_enabled", "true")
        target_google_search = json_file.get("target_google_search", "true")
        target_search_network = json_file.get("target_search_network", "true")
        target_partner_search_network = json_file.get("target_partner_search_network", "false")
        target_content_network = json_file.get("target_content_network", "true")

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"add_campaign: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.add_campaign(customer_id=customer_id,
                                       campaign_budget_name=campaign_budget_name,
                                       campaign_budget_amount=campaign_budget_amount,
                                       campaign_name=campaign_name,
                                       date_from=date_from,
                                       date_to=date_to,
                                       enhanced_cpc_enabled=to_boolean(enhanced_cpc_enabled),
                                       target_google_search=to_boolean(target_google_search),
                                       target_search_network=to_boolean(target_search_network),
                                       target_partner_search_network=to_boolean(target_partner_search_network),
                                       target_content_network=to_boolean(target_content_network)
                                       )
            return jsonify(result)

    except BadRequestKeyError:
        logger.error("add_campaign: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("add_campaign: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'add_campaign: {ex}')
        raise HttpError(400, f'{ex}')


@app.route('/googleads/addadgroup', methods=['POST'])
@swag_from("swagger_conf/add_adgroup.yml")
def add_adgroup():
    """Добавить группу"""

    try:
        json_file = request.get_json(force=False)
        login_customer_id = json_file["login_customer_id"]
        refresh_token = json_file["refresh_token"]
        customer_id = json_file["customer_id"]
        campaign_id = json_file["campaign_id"]
        name = json_file["name"]
        cpc_bid = json_file["cpc_bid"]

        gads = GAdsEcomru(
            client_id=config.CLIENT_ID,
            client_secret=config.CLIENT_SECRET,
            developer_token=config.DEVELOPER_TOKEN,
            login_customer_id=login_customer_id,
            refresh_token=refresh_token
        )

        if gads.client is None:
            logger.error(
                f"add_adgroup: client initialization error - incorrect login_customer_id and (or) refresh_token")
            return jsonify(
                {"error": "client initialization error",
                 "message": "incorrect login_customer_id and (or) refresh_token"}
            )

        else:
            result = gads.create_ad_group(customer_id=customer_id, campaign_id=campaign_id, name=name, cpc_bid=cpc_bid)

            return jsonify(result)

    except BadRequestKeyError:
        logger.error("add_adgroup: BadRequest")
        return Response(None, 400)
    except KeyError:
        logger.error("add_adgroup: KeyError")
        return Response(None, 400)
    except BaseException as ex:
        logger.error(f'add_adgroup: {ex}')
        raise HttpError(400, f'{ex}')



