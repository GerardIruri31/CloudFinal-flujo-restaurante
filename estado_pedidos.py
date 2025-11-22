import os
import json
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")

TABLA_PEDIDOS = os.getenv("TABLA_PEDIDOS", "PEDIDOS")
TABLA_COCINA = os.getenv("TABLA_COCINA", "COCINA")
TABLA_DESPACHADOR = os.getenv("TABLA_DESPACHADOR", "DESPACHADOR")
TABLA_DELIVERY = os.getenv("TABLA_DELIVERY", "DELIVERY")

tabla_pedidos = dynamodb.Table(TABLA_PEDIDOS)
tabla_cocina = dynamodb.Table(TABLA_COCINA)
tabla_despachador = dynamodb.Table(TABLA_DESPACHADOR)
tabla_delivery = dynamodb.Table(TABLA_DELIVERY)


# ------------------------- Utilitarios ------------------------- #

def obtener_timestamp_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_event(event):
    """
    Normaliza el event para que siempre trabajemos con un dict simple:
    - Si viene de API Gateway HTTP API (Postman), el body viene en event["body"] (string).
    - Si viene de Step Functions u otra Lambda, ya es un dict.
    También mezcla pathParameters (ej: id_pedido del path).
    """
    # Caso típico HTTP API / Postman
    if "body" in event and isinstance(event["body"], str):
        try:
            body = json.loads(event["body"])
        except Exception:
            # Si el body no es JSON válido, devolvemos dict vacío
            body = {}

        # Mezclamos pathParameters si existen (por ejemplo, id_pedido del path)
        path_params = event.get("pathParameters") or {}
        if isinstance(path_params, dict):
            for k, v in path_params.items():
                # Si no viene en el body, añadimos desde path
                body.setdefault(k, v)

        return body

    # Caso Step Functions u otro servicio que ya mande dict
    # Si ahí quieren mandar tenant_id, id_pedido, etc. directamente.
    return event


def obtener_pedido(tenant_id: str, id_pedido: str):
    resp = tabla_pedidos.get_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        }
    )
    return resp.get("Item")


def validar_pedido_y_estado(event, estado_esperado: str):
    """
    Devuelve (pedido, error_response | None)
    Si hay error, pedido será None y error_response será el dict de respuesta HTTP.
    """
    tenant_id = event.get("tenant_id")
    id_pedido = event.get("id_pedido") or event.get("id")  # por si mandas "id"

    if not tenant_id or not id_pedido:
        return None, {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Faltan tenant_id o id_pedido en el event"
            })
        }

    pedido = obtener_pedido(tenant_id, id_pedido)
    if not pedido:
        return None, {
            "statusCode": 404,
            "body": json.dumps({
                "mensaje": "Pedido no encontrado",
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            })
        }

    estado_actual = pedido.get("estado_pedido")
    if estado_actual != estado_esperado:
        return None, {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": f"Estado actual del pedido es '{estado_actual}', "
                           f"pero esta Lambda espera '{estado_esperado}'"
            })
        }

    return pedido, None


def actualizar_estado_pedido(tenant_id: str, id_pedido: str, nuevo_estado: str):
    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression="SET estado_pedido = :e",
        ExpressionAttributeValues={":e": nuevo_estado}
    )


# ------------------------- Lambda 1: pagado -> cocina ------------------------- #

def pagado_a_cocina(event, context):
    """
    Transición:
      pagado -> cocina
      - Crea registro en COCINA
      - Actualiza PEDIDOS.estado_pedido = 'cocina'
    """
    # Normalizar event (body de Postman, pathParameters, etc.)
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "pagado")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]
    id_empleado = event.get("id_empleado")

    item_cocina = {
        "id_pedido": id_pedido,
        "id_empleado": id_empleado or "no_asignado",
        "hora_comienzo": obtener_timestamp_iso(),
        "hora_fin": None,
        "status": "cocinando"
    }

    tabla_cocina.put_item(Item=item_cocina)

    actualizar_estado_pedido(tenant_id, id_pedido, "cocina")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición pagado -> cocina realizada",
            "pedido": {
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            },
            "registro_cocina": item_cocina
        })
    }


# ------------------------- Lambda 2: cocina -> empaquetamiento ------------------------- #

def cocina_a_empaquetamiento(event, context):
    """
    Transición:
      cocina -> empaquetamiento
      - Actualiza COCINA (fin de cocción)
      - Crea registro en DESPACHADOR (empaquetamiento)
      - Actualiza PEDIDOS.estado_pedido = 'empaquetamiento'
    """
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "cocina")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]
    id_empleado_despachador = event.get("id_empleado")

    # 1) Terminar en COCINA
    tabla_cocina.update_item(
        Key={"id_pedido": id_pedido},
        UpdateExpression="SET hora_fin = :hf, #st = :s",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":hf": obtener_timestamp_iso(),
            ":s": "terminado"
        }
    )

    # 2) Crear registro en DESPACHADOR (empaquetamiento)
    item_despachador = {
        "id_pedido": id_pedido,
        "id_empleado": id_empleado_despachador or "no_asignado",
        "hora_comienzo": obtener_timestamp_iso(),
        "hora_fin": None,
        "status": "cocinando"  # cambia el texto si quieres algo tipo "empaquetando"
    }

    tabla_despachador.put_item(Item=item_despachador)

    # 3) Actualizar estado del pedido
    actualizar_estado_pedido(tenant_id, id_pedido, "empaquetamiento")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición cocina -> empaquetamiento realizada",
            "pedido": {
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            },
            "detalle": {
                "cocina": {
                    "id_pedido": id_pedido,
                    "status": "terminado"
                },
                "despachador": item_despachador
            }
        })
    }


# ------------------------- Lambda 3: empaquetamiento -> delivery ------------------------- #

def empaquetamiento_a_delivery(event, context):
    """
    Transición:
      empaquetamiento -> delivery
      - Actualiza DESPACHADOR (fin empaquetamiento)
      - Crea registro en DELIVERY
      - Actualiza PEDIDOS.estado_pedido = 'delivery'
    """
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "empaquetamiento")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]

    repartidor = event.get("repartidor")
    id_repartidor = event.get("id_repartidor")
    origen = event.get("origen")
    destino = event.get("destino")

    # 1) Terminar empaquetamiento
    tabla_despachador.update_item(
        Key={"id_pedido": id_pedido},
        UpdateExpression="SET hora_fin = :hf, #st = :s",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":hf": obtener_timestamp_iso(),
            ":s": "terminado"
        }
    )

    # 2) Crear registro en DELIVERY
    item_delivery = {
        "id_pedido": id_pedido,
        "tenant_id": tenant_id,
        "repartidor": repartidor or "no_asignado",
        "id_repartidor": id_repartidor or "no_asignado",
        "origen": origen or "no_definido",
        "destino": destino or "no_definido",
        "status": "en camino"
    }

    tabla_delivery.put_item(Item=item_delivery)

    # 3) Actualizar estado del pedido
    actualizar_estado_pedido(tenant_id, id_pedido, "delivery")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición empaquetamiento -> delivery realizada",
            "pedido": {
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            },
            "detalle": {
                "despachador": {
                    "id_pedido": id_pedido,
                    "status": "terminado"
                },
                "delivery": item_delivery
            }
        })
    }


# ------------------------- Lambda 4: delivery -> entregado ------------------------- #

def delivery_a_entregado(event, context):
    """
    Transición:
      delivery -> entregado
      - Actualiza DELIVERY.status = 'cumplido'
      - Actualiza PEDIDOS.estado_pedido = 'entregado'
    """
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "delivery")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]

    # 1) Actualizar DELIVERY
    tabla_delivery.update_item(
        Key={"id_pedido": id_pedido},
        UpdateExpression="SET #st = :s",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":s": "cumplido"}
    )

    # 2) Actualizar estado en PEDIDOS
    actualizar_estado_pedido(tenant_id, id_pedido, "entregado")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición delivery -> entregado realizada",
            "pedido": {
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            },
            "delivery": {
                "id_pedido": id_pedido,
                "status": "cumplido"
            }
        })
    }

