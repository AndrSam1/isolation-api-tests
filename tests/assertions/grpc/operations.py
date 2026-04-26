import allure

from contracts.services.operations.operation_pb2 import Operation
from contracts.services.operations.rpc_get_operations_pb2 import GetOperationsResponse
from tests.assertions.base import assert_equal
from tests.schema.operations import OperationEventTestSchema
from tests.clients.postgres.operations.model import OperationsTestModel
from tests.tools.date import to_proto_test_datetime
from tests.tools.logger import get_test_logger
from tests.types.operations import OperationTestType, OperationTestStatus

logger = get_test_logger("OPERATIONS_ASSERTIONS")

OPERATION_TYPE_MAPPING = {
    OperationTestType.FEE: 1,
    OperationTestType.TOP_UP: 3,
    OperationTestType.PURCHASE: 4,
    OperationTestType.CASHBACK: 5,
    OperationTestType.TRANSFER: 6,
    OperationTestType.REVERSAL: 7,
    OperationTestType.UNSPECIFIED: 0,
    OperationTestType.BILL_PAYMENT: 8,
    OperationTestType.CASH_WITHDRAWAL: 9,
}

OPERATION_STATUS_MAPPING = {
    OperationTestStatus.FAILED: 4,
    OperationTestStatus.REVERSED: 3,
    OperationTestStatus.COMPLETED: 2,
    OperationTestStatus.IN_PROGRESS: 1,
    OperationTestStatus.UNSPECIFIED: 0,
}


@allure.step("Check operation from event")
def assert_operation_from_event(actual: Operation, expected: OperationEventTestSchema) -> None:
    """
    Проверяет protobuf-модель операции Operation на соответствие
    доменной схеме события OperationEventTestSchema.

    Такой ассерт используется в сценариях на основе событий,
    где ожидаемые данные формируются на основе Kafka-события.

    Особенности:
    - проверяются только доменные атрибуты операции;
    - идентификатор операции может быть сгенерирован системой и не проверяется.
    """
    logger.info("Check operation from event")

    # Преобразуем доменные значения enum'ов в protobuf-формат с помощью маппинга
    expected_type = OPERATION_TYPE_MAPPING[expected.type]
    expected_status = OPERATION_STATUS_MAPPING[expected.status]

    assert_equal(actual.type, expected_type, "type")
    assert_equal(actual.status, expected_status, "status")
    assert_equal(actual.amount, expected.amount, "amount")
    assert_equal(str(actual.user_id), str(expected.user_id), "user_id")
    assert_equal(str(actual.card_id), str(expected.card_id), "card_id")
    assert_equal(actual.category, expected.category, "category")
    assert_equal(actual.created_at, to_proto_test_datetime(expected.created_at), "created_at")
    assert_equal(str(actual.account_id), str(expected.account_id), "account_id")


@allure.step("Check operation from model")
def assert_operation_from_model(actual: Operation, expected: OperationsTestModel) -> None:
    """
    Проверяет protobuf-модель операции Operation на соответствие
    модели базы данных OperationsTestModel.

    Такой ассерт используется в сценариях на основе сидинга,
    где ожидаемые данные формируются на основе модели Postgres.

    Особенности:
    - проверяется идентификатор операции как часть контракта;
    - сравниваются все ключевые доменные поля.
    """
    logger.info("Check operation from model")

    # Преобразуем доменные значения enum'ов в protobuf-формат с помощью маппинга
    expected_type = OPERATION_TYPE_MAPPING[OperationTestType(expected.type)]
    expected_status = OPERATION_STATUS_MAPPING[OperationTestStatus(expected.status)]

    assert_equal(str(actual.id), str(expected.id), "id")
    assert_equal(actual.type, expected_type, "type")
    assert_equal(actual.status, expected_status, "status")
    assert_equal(actual.amount, expected.amount, "amount")
    assert_equal(str(actual.user_id), str(expected.user_id), "user_id")
    assert_equal(str(actual.card_id), str(expected.card_id), "card_id")
    assert_equal(actual.category, expected.category, "category")
    assert_equal(actual.created_at, to_proto_test_datetime(expected.created_at), "created_at")
    assert_equal(str(actual.account_id), str(expected.account_id), "account_id")


@allure.step("Check get operations response from events")
def assert_get_operations_response_from_events(actual: GetOperationsResponse,
                                               expected: list[OperationEventTestSchema]) -> None:
    """
    Проверяет gRPC-ответ GetOperationsResponse на соответствие
    списку доменных схем событий OperationEventTestSchema.

    Используется в сценариях на основе событий.

    Особенности:
    - проверяется количество операций в ответе;
    - каждая операция проверяется через assert_operation_from_event.
    """
    logger.info("Check get operations response from events")

    assert_equal(len(actual.operations), len(expected), "operations count")

    for index, expected_operation in enumerate(expected):
        assert_operation_from_event(actual.operations[index], expected_operation)


@allure.step("Check get operations response from models")
def assert_get_operations_response_from_models(actual: GetOperationsResponse,
                                               expected: list[OperationsTestModel]) -> None:
    """
    Проверяет gRPC-ответ GetOperationsResponse на соответствие
    списку моделей базы данных OperationsTestModel.

    Используется в сценариях на основе сидинга.

    Особенности:
    - проверяется количество операций в ответе;
    - каждая операция проверяется через assert_operation_from_model.
    """
    logger.info("Check get operations response from models")

    assert_equal(len(actual.operations), len(expected), "operations count")

    for index, expected_operation in enumerate(expected):
        assert_operation_from_model(actual.operations[index], expected_operation)
