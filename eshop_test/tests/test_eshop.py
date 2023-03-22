from datetime import time
import datetime
import pytest
from utils import *
import json
import uuid
import json
from time import sleep
from utils.api.bearer_tokenizer import BearerTokenizer
from utils.db.db_utils import MSSQLConnector
from utils.docker.docker_utils import DockerManager
from utils.rabbitmq.rabbitmq_send import RabbitMQ
from utils.api.ordering_api import OrderingAPI
from utils.rabbitmq.rabbitmq_receive import *


@pytest.fixture()
def docker():
    return DockerManager()

def get_last_order_id_from_db():
    '''
        Gets the id of the last order in DB
    '''
    with MSSQLConnector() as conn:
        last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']
        return last_order_id


def count_orders_in_db():
    '''
        Gets number of orders in db
    '''
    with MSSQLConnector() as conn:
        count_orders = conn.select_query('SELECT COUNT(Id) from ordering.orders')[0]['']
        return count_orders


def create_msg_checkout(quantity=1, cardtype=1, card_secuity_number=123):
    '''
        Creates the checkout message for sending to rabbitMQ
    '''
    msg = {
            "UserId": "b9e5dcdd-dae2-4b1c-a991-f74aae042814",
            "UserName": "alice",
            "OrderNumber": 0,
            "City": "Redmond",
            "Street": "15703 NE 61st Ct",
            "State": "WA",
            "Country": "U.S.",
            "ZipCode": "98052",
            "CardNumber": "4012888888881881",
            "CardHolderName": "Alice Smith",
            "CardExpiration": "2024-08-31T22:00:00Z",
            "CardSecurityNumber": card_secuity_number,
            "CardTypeId": cardtype,
            "Buyer": 'null',
            "RequestId": str(uuid.uuid4()),
            "Basket": {
                "BuyerId": "b9e5dcdd-dae2-4b1c-a991-f74aae042814",
                "Items": [
                    {
                        "Id": "c1f98125-a109-4840-a751-c12a77f58dff",
                        "ProductId": 1,
                        "ProductName": ".NET Bot Black Hoodie",
                        "UnitPrice": 19.5,
                        "OldUnitPrice": 0,
                        "Quantity": quantity,
                        "PictureUrl": "http://host.docker.internal:5202/c/api/v1/catalog/items/1/pic/"
                    }
                ]
            },
            "Id": "16c5ddbc-229e-4c19-a4bd-d4148417529c",
            "CreationDate": "2023-03-04T14:20:24.4730559Z"
        }
    return msg


@pytest.mark.success_flow
def test_success_flow():
    '''
        Checks the creation of a new order 
    '''

    print("mylog: starting success flow")

    # Creates the json msg to send to RabbitMQ
    msg = create_msg_checkout()

    # Reads from database number of orders.
    # In this test we add a new order so at end of this test
    # we will check that this number of orders is increased by one.
    count_orders = count_orders_in_db()

    print(f"mylog: count_orders from db = " + str(count_orders))

    with RabbitMQ() as mq:
        mq.publish(exchange='eshop_event_bus',
                   routing_key='UserCheckoutAcceptedIntegrationEvent',
                   body=json.dumps(msg))
        sleep(4)
        mq.consume('Basket', callback)
        sleep(3)
        assert returnglob() == 'OrderStartedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert order_status_of_last_order[0]['OrderStatusId'] == 1

    with RabbitMQ() as mq:
        mq.consume('Catalog', callback)
        sleep(3)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        #sleep(4)
        assert order_status_of_last_order[0]['OrderStatusId'] == 2

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    last_order_id = ''
    with MSSQLConnector() as conn:
        last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']

    with RabbitMQ() as mq:
        msg={
            "OrderId": last_order_id,
            "Id": "e9b80940-c861-4e5b-9d7e-388fd256acef",
            "CreationDate": "2023-03-07T09:52:56.6412897Z"
        }
        mq.publish(exchange='eshop_event_bus',
                    routing_key='OrderStockConfirmedIntegrationEvent',
                    body=json.dumps(msg))

        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Payment', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert order_status_of_last_order[0]['OrderStatusId'] == 3

    with RabbitMQ() as mq:
        msg = {
            "OrderId": get_last_order_id_from_db(),
            "Id": "b84dc7a5-1d0e-429e-a800-d3024d9c724f",
            "CreationDate": "2023-03-05T15:33:18.1376971Z"
        }
        mq.publish(exchange='eshop_event_bus',
                    routing_key='OrderPaymentSucceededIntegrationEvent',
                    body=json.dumps(msg))
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToPaidIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Catalog', callback)
        assert returnglob() == 'OrderStatusChangedToPaidIntegrationEvent'

    with MSSQLConnector() as conn:
        result = conn.select_query('SELECT COUNT(Id) from ordering.orders')
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert result[0][''] == count_orders + 1
        assert order_status_of_last_order[0]['OrderStatusId'] == 4

    # assert count_orders == 0


@pytest.mark.out_of_stock_flow
def test_out_of_stock_flow():
    '''
        CheckS the creation of a new order without enough items in stock
    '''
    msg = create_msg_checkout(quantity=10000)

    count_orders = count_orders_in_db()

    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(msg))
            mq.consume('Basket', callback)
            returnglob2 = returnglob()
            returnglob1 = returnglob2
            assert returnglob1 == 'OrderStartedIntegrationEvent'

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob1 == 'OrderStatusChangedToSubmittedIntegrationEvent'

            mq.consume('Catalog', callback)
            assert returnglob1 == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob1 == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

            msg = {
                "OrderId": get_last_order_id_from_db(),
                "OrderStockItems": [
                    {
                        "ProductId": 1,
                        "HasStock": False
                    }
                ],
                "Id": "99c3f974-c6ed-41a4-8e01-5cb00f9e6335",
                "CreationDate": "2023-03-05T15:51:11.5458796Z"
            }
            mq.publish(exchange='eshop_event_bus',
                       routing_key='OrderStockRejectedIntegrationEvent',
                       body=json.dumps(msg))
    sleep(15)
    with MSSQLConnector() as conn:
        result = conn.select_query('SELECT COUNT(Id) from ordering.orders')
        orderstatus = conn.select_query(
            'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
    assert result[0][''] == count_orders + 1
    assert orderstatus[0]['OrderStatusId'] == 6


@pytest.mark.payment_failure_flow
def test_payment_failure_flow():
    '''
        CheckS the creation of a new order with a payment failure 
    '''

    msg = create_msg_checkout()

    count_orders = count_orders_in_db()

    # with MSSQLConnector() as conn:
    #     with RabbitMQ() as mq:

    with RabbitMQ() as mq:
        mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                   body=json.dumps(msg))
        mq.consume('Basket', callback)
        assert returnglob() == 'OrderStartedIntegrationEvent'

    with MSSQLConnector() as conn:
        orderstatus = conn.select_query(
            'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert orderstatus[0]['OrderStatusId'] == 1

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Catalog', callback)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    with MSSQLConnector() as conn:
        orderstatus = conn.select_query(
            'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert orderstatus[0]['OrderStatusId'] == 2

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    last_order_id = ''
    with MSSQLConnector() as conn:
        last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']

    with RabbitMQ() as mq:
        msg = {
            "OrderId": last_order_id,
            "Id": "e9b80940-c861-4e5b-9d7e-388fd256acef",
            "CreationDate": "2023-03-07T09:52:56.6412897Z"
        }
        mq.publish(exchange='eshop_event_bus', routing_key='OrderStockConfirmedIntegrationEvent',
                   body=json.dumps(msg))

        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Payment', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with MSSQLConnector() as conn:
        orderstatus = conn.select_query(
            'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert orderstatus[0]['OrderStatusId'] == 3

    with RabbitMQ() as mq:
        msg = {
            "OrderId": get_last_order_id_from_db(),
            "OrderStatus": "stockconfirmed",
            "BuyerName": "alice",
            "Id": "cca155c0-4480-4c93-a763-910e54218040",
            "CreationDate": "2023-03-05T17:07:35.6306122Z"
        }
        mq.publish(exchange='eshop_event_bus',
                   routing_key='OrderPaymentFailedIntegrationEvent',
                   body=json.dumps(msg))

    with MSSQLConnector() as conn:
        result = conn.select_query('SELECT COUNT(Id) from ordering.orders')
        while True:
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            if orderstatus[0]['OrderStatusId'] != 3:
                break
            sleep(1)

    with RabbitMQ() as mq:
        mq.purge('Ordering.signalrhub')

    assert result[0][''] == count_orders + 1
    assert orderstatus[0]['OrderStatusId'] == 6


@pytest.mark.cancel_order_status_1
def test_cancel_order_status_1():
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(create_msg_checkout()))
            sleep(3)
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 1

            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 1')
            orderid = orderid[0]['']
            OrderingAPI().cancel_order(orderid)

            mq.purge('Basket')
            mq.purge('Ordering.signalrhub')
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')
    try:
        assert newstatus[0]['OrderStatusId'] == 6
        print('test_cancel_order_status_1: test pass')
    except:
        print('test_cancel_order_status_1: test fail')


@pytest.mark.cancel_order_status_2
def test_cancel_order_status_2():
    body = create_msg_checkout()
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            mq.consume('Basket', callback)
            assert returnglob() == 'OrderStartedIntegrationEvent'

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 1

            # mq.consume('Catalog', callback)
            # assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'
            while True:
                orderstatus = conn.select_query(
                    'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
                if orderstatus[0]['OrderStatusId'] == 2:
                    break
                sleep(2)

            assert orderstatus[0]['OrderStatusId'] == 2

            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 2')
            orderid = orderid[0]['']
            OrderingAPI().cancel_order(orderid)
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')
            mq.purge('Catalog')
    try:
        assert newstatus[0]['OrderStatusId'] == 6
        print('test_cancel_order_status_2: test pass')
    except:
        print('test_cancel_order_status_2: test fail')


@pytest.mark.cancel_order_status_3
def test_cancel_order_status_3():
    body = create_msg_checkout()
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            mq.consume('Basket', callback)
            assert returnglob() == 'OrderStartedIntegrationEvent'

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 1

            mq.consume('Catalog', callback)
            assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            # sleep(5)
            assert orderstatus[0]['OrderStatusId'] == 2

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

            last_order_id = ''
            with MSSQLConnector() as conn:
                last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']
            msg = {
                "OrderId": last_order_id,
                "Id": "e9b80940-c861-4e5b-9d7e-388fd256acef",
                "CreationDate": "2023-03-07T09:52:56.6412897Z"
            }

            mq.publish(exchange='eshop_event_bus', routing_key='OrderStockConfirmedIntegrationEvent',
                       body=json.dumps(body))

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

            # mq.consume('Payment', callback)
            # assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 3

            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 3')
            orderid = orderid[0]['']
            OrderingAPI().cancel_order(orderid)
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')
            mq.purge('Payment')
    try:
        assert newstatus[0]['OrderStatusId'] == 6
        print('test_cancel_order_status_3: test pass')
    except:
        print('test_cancel_order_status_3: test fail')


@pytest.mark.cancel_order_status_4_fail
def test_cancel_order_status_4_fail():
    with MSSQLConnector() as conn:
        test_success_flow()
        orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 4')
        orderid = orderid[0]['']
        OrderingAPI().cancel_order(orderid)
        newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')
    assert newstatus[0]['OrderStatusId'] == 4


@pytest.mark.cancel_order_status_5_fail
def test_cancel_order_status_5_fail():
    with MSSQLConnector() as conn:
        test_update_order_to_shiped()
        orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 5')
        orderid = orderid[0]['']
        OrderingAPI().cancel_order(orderid)
        newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')
    assert newstatus[0]['OrderStatusId'] == 5


@pytest.mark.update_order_to_shiped
def test_update_order_to_shiped():
    test_success_flow()
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 4')
            orderid = orderid[0]['']
            OrderingAPI().update_to_shiped(orderid)
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')
    try:
        assert newstatus[0]['OrderStatusId'] == 5
        print('test_update_order_to_shiped: test pass')
    except:
        print('test_update_order_to_shiped: test fail')


@pytest.mark.update_order_to_shiped_fail_status_1
def test_update_order_to_shiped_fail_status_1():
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            count_orders = count_orders_in_db()

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(create_msg_checkout()))
            sleep(3)
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 1
            sleep(3)
            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 1')
            orderid = orderid[0]['']

            mq.purge('Basket')
            mq.purge('Ordering.signalrhub')
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')
        try:
            assert newstatus[0]['OrderStatusId'] == 1
            print('test_cancel_order_status_1: test pass')
        except:
            print('test_cancel_order_status_1: test fail')


@pytest.mark.update_order_to_shiped_fail_status_2
def test_update_order_to_shiped_fail_status_2():
    body = create_msg_checkout()
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            count_orders = count_orders_in_db()

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            mq.consume('Basket', callback)
            assert returnglob() == 'OrderStartedIntegrationEvent'

            while True:
                orderstatus = conn.select_query(
                    'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
                if orderstatus[0]['OrderStatusId'] == 2:
                    break
                sleep(1)

            mq.consume('Catalog', callback)
            assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            sleep(5)
            assert orderstatus[0]['OrderStatusId'] == 2

            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 2')
            orderid = orderid[0]['']
            OrderingAPI().update_to_shiped(orderid)
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')
        assert newstatus[0]['OrderStatusId'] == 2


@pytest.mark.update_order_to_shiped_fail_status_3
def test_update_order_to_shiped_fail_status_3():
    body = create_msg_checkout()
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            count_orders = count_orders_in_db()

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            mq.consume('Basket', callback)
            assert returnglob() == 'OrderStartedIntegrationEvent'

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 1

            mq.consume('Catalog', callback)
            assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            # sleep(5)
            assert orderstatus[0]['OrderStatusId'] == 2

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

            last_order_id = ''
            with MSSQLConnector() as conn:
                last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']
            msg = {
                "OrderId": last_order_id,
                "Id": "e9b80940-c861-4e5b-9d7e-388fd256acef",
                "CreationDate": "2023-03-07T09:52:56.6412897Z"
            }
            mq.publish(exchange='eshop_event_bus',
                       routing_key='OrderStockConfirmedIntegrationEvent',
                       body=json.dumps(msg))

            mq.consume('Ordering.signalrhub', callback)
            assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

            # mq.consume('Payment', callback)
            # assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'
            orderstatus = conn.select_query(
                'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
            assert orderstatus[0]['OrderStatusId'] == 3

            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 3')
            orderid = orderid[0]['']

            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')
            mq.purge('Payment')
    try:
        assert newstatus[0]['OrderStatusId'] == 3
        print('test_cancel_order_status_3: test pass')
    except:
        print('test_cancel_order_status_3: test fail')


@pytest.mark.update_order_to_shiped_fail_status_6
def test_update_order_to_shiped_fail_status_6():
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            test_cancel_order_status_1()
            orderid = conn.select_query('SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 6')
            orderid = orderid[0]['']
            OrderingAPI().update_to_shiped(orderid)

            while True:
                orderstatus = conn.select_query(
                    'select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
                if orderstatus[0]['OrderStatusId'] == 6:
                    break
                sleep(1)

            mq.purge('Catalog')
            mq.purge('Ordering.signalrhub')
    try:
        assert orderstatus[0]['OrderStatusId'] == 6
        print('test_update_order_to_shiped_fail_status_6: test pass')
    except:
        print('test_update_order_to_shiped_fail_status_6: test fail')


@pytest.mark.order_fail_with_card_type_4
def test_order_fail_with_card_type_4():
    body = create_msg_checkout(cardtype=4)
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            startcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            endingcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')
    try:
        assert startcount == endingcount
        print('test_order_fail_with_card_type_4: test pass no new order has created')
    except:
        print('test_order_fail_with_card_type_4: test fail a order has been created')


@pytest.mark.order_fail_with_card_type_0
def test_order_fail_with_card_type_0():
    body = create_msg_checkout(cardtype=0)
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            startcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            endingcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')
    try:
        assert startcount == endingcount
        print('test_order_fail_with_card_type_0: test pass no new order has created')
    except:
        print('test_order_fail_with_card_type_0: test fail a order has been created')


@pytest.mark.order_fail_with_card_type_negative
def test_order_fail_with_card_type_negative():
    body = create_msg_checkout(cardtype=-1)
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            startcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))

            endingcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')
    try:
        assert startcount == endingcount
        print('test_order_fail_with_card_type_negative: test pass')
    except:
        print('test_order_fail_with_card_type_negative: test fail')


@pytest.mark.create_order_with_wrong_security_number
def test_create_order_with_wrong_security_number():
    body = create_msg_checkout(card_secuity_number=9999)
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            startcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))
            sleep(10)
            endingcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.purge('Basket')
            mq.purge('Catalog')
            mq.purge('Ordering.signalrhub')

    try:
        assert startcount == endingcount
        print('test_order_fail_with_card_type_negative: test pass')
    except:
        print('test_order_fail_with_card_type_negative: test fail')


@pytest.mark.create_order_with_wrong_credit_card_number
def test_create_order_with_wrong_credit_card_number():
    body = create_msg_checkout(cardnumber=9999999999999999)
    with MSSQLConnector() as conn:
        with RabbitMQ() as mq:
            startcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.publish(exchange='eshop_event_bus', routing_key='UserCheckoutAcceptedIntegrationEvent',
                       body=json.dumps(body))
            sleep(5)
            # mq.consume('Basket', callback)
            # assert returnglob() == 'OrderStartedIntegrationEvent'

            # mq.consume('Ordering.signalrhub', callback)
            # assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'
            endingcount = conn.select_query('SELECT COUNT(Id) from ordering.orders')

            mq.purge('Basket')
            mq.purge('Catalog')
            mq.purge('Ordering.signalrhub')

    try:
        assert startcount == endingcount
        print('test_order_fail_with_card_type_negative: test pass')
    except:
        print('test_order_fail_with_card_type_negative: test fail')


def test_get_user_order_by_id():
    ordernumber = 99
    msg = {
        "ordernumber": ordernumber,
        "date": "2023-03-13T10:47:52.5204089",
        "status": "shipped",
        "description": None,
        "street": "15703 NE 61st Ct",
        "city": "Redmond",
        "zipcode": "98052",
        "country": "U.S.",
        "orderitems": [
            {
                "productname": ".NET Black & White Mug",
                "units": 1,
                "unitprice": 8.5,
                "pictureurl": "http://host.docker.internal:5202/c/api/v1/catalog/items/2/pic/"
            },
            {
                "productname": ".NET Blue Hoodie",
                "units": 1,
                "unitprice": 12,
                "pictureurl": "http://host.docker.internal:5202/c/api/v1/catalog/items/6/pic/"
            }
        ],
        "total": 20.50
    }
    respone = OrderingAPI().get_order_by_id(ordernumber).json()
    assert respone == msg


def test_get_user_order_by_id_of_diffrent_user():
    orderid = 42
    assert OrderingAPI().get_order_by_id(orderid).status_code == 401


def test_get_user_orders(order_api):
    order_api = OrderingAPI('bob', 'Pass123%24')
    orders = len(order_api.get_orders().json())
    assert orders == 2


@pytest.mark.update_order_to_shiped
def test_update_order_to_shiped_to_diffrent_user():
    username = 'john'
    with RabbitMQ() as mq:
        with MSSQLConnector() as conn:
            orderid = conn.select_query(
                'SELECT MAX(Id) from ordering.orders where orders.OrderStatusId = 4 and Id != 11')
            orderid = orderid[0]['']
            OrderingAPI().update_to_shiped(orderid)
            sleep(5)
            newstatus = conn.select_query(f'SELECT OrderStatusId from ordering.orders where Id = {orderid}')

            mq.purge('Ordering.signalrhub')

        assert newstatus[0]['OrderStatusId'] == 4


@pytest.mark.requirementscalibility
def test_many_requests():


    with RabbitMQ() as mq:
        with MSSQLConnector() as sql:
            last_id = get_last_order_id_from_db()
            print(last_id)
            # step 1
            now = datetime.datetime.now()
            for i in range(1, 101):
                message = create_msg_checkout()
                mq.publish(exchange='eshop_event_bus',
                           routing_key=message["RoutingKey"],
                           body=json.dumps(message["Body"]))
            time_of_mission = datetime.datetime.now()
            diff_minutes = time_of_mission - now / 60
            while diff_minutes <= 360:
                currentid = get_last_order_id_from_db()
                diff_minutes = time_of_mission - now / 60
                if currentid <= last_id + 100:
                    break

            assert sql.req_query(f'SELECT amount from (SELECT count(Id) as amount, OrderStatusId from '
                                 f'ordering.orders where Id > ({last_id}) GROUP BY OrderStatusId) o where'
                                 f' OrderStatusId = 4')[0]['amount'] == 100





@pytest.mark.success_flowwithstop
def test_success_flowwithstop():
    '''
        Checks the creation of a new order
    '''

    print("mylog: starting success flow")

    # Creates the json msg to send to RabbitMQ
    msg = create_msg_checkout()

    # Reads from database number of orders.
    # In this test we add a new order so at end of this test
    # we will check that this number of orders is increased by one.
    count_orders = count_orders_in_db()

    print(f"mylog: count_orders from db = " + str(count_orders))

    with RabbitMQ() as mq:
        mq.publish(exchange='eshop_event_bus',
                   routing_key='UserCheckoutAcceptedIntegrationEvent',
                   body=json.dumps(msg))
        mq.close()
        mq.connect()
        sleep(4)
        mq.consume('Basket', callback)
        sleep(3)
        assert returnglob() == 'OrderStartedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToSubmittedIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert order_status_of_last_order[0]['OrderStatusId'] == 1

    with RabbitMQ() as mq:
        mq.consume('Catalog', callback)
        sleep(3)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        #sleep(4)
        assert order_status_of_last_order[0]['OrderStatusId'] == 2

    with RabbitMQ() as mq:
        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToAwaitingValidationIntegrationEvent'

    last_order_id = ''
    with MSSQLConnector() as conn:
        last_order_id = conn.select_query('SELECT MAX(Id) FROM ordering.orders')[0]['']

    with RabbitMQ() as mq:
        msg={
            "OrderId": last_order_id,
            "Id": "e9b80940-c861-4e5b-9d7e-388fd256acef",
            "CreationDate": "2023-03-07T09:52:56.6412897Z"
        }
        mq.publish(exchange='eshop_event_bus',
                    routing_key='OrderStockConfirmedIntegrationEvent',
                    body=json.dumps(msg))

        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Payment', callback)
        assert returnglob() == 'OrderStatusChangedToStockConfirmedIntegrationEvent'

    with MSSQLConnector() as conn:
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert order_status_of_last_order[0]['OrderStatusId'] == 3

    with RabbitMQ() as mq:
        msg = {
            "OrderId": get_last_order_id_from_db(),
            "Id": "b84dc7a5-1d0e-429e-a800-d3024d9c724f",
            "CreationDate": "2023-03-05T15:33:18.1376971Z"
        }
        mq.publish(exchange='eshop_event_bus',
                    routing_key='OrderPaymentSucceededIntegrationEvent',
                    body=json.dumps(msg))

        mq.consume('Ordering.signalrhub', callback)
        assert returnglob() == 'OrderStatusChangedToPaidIntegrationEvent'

    with RabbitMQ() as mq:
        mq.consume('Catalog', callback)
        assert returnglob() == 'OrderStatusChangedToPaidIntegrationEvent'

    with MSSQLConnector() as conn:
        result = conn.select_query('SELECT COUNT(Id) from ordering.orders')
        order_status_of_last_order = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        assert result[0][''] == count_orders + 1
        assert order_status_of_last_order[0]['OrderStatusId'] == 4

    # assert count_orders == 0



