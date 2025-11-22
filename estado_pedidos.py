import os
import json
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
stepfunctions_client = boto3.client("stepfunctions")

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
    Normaliza el event para:
    - HTTP API (Postman): body JSON en event["body"]
    - Step Functions con waitForTaskToken: { "taskToken": "...", "input": {...} }
    - Step Functions normal: input directo
    """
    # Caso Step Functions con waitForTaskToken
    if "taskToken" in event and "input" in event and isinstance(event["input"], dict):
        base = event["input"].copy()
        base["taskToken"] = event["taskToken"]
        return base

    # Caso típico HTTP API / Postman
    if "body" in event and isinstance(event["body"], str):
        try:
            body = json.loads(event["body"])
        except Exception:
            body = {}

        path_params = event.get("pathParameters") or {}
        if isinstance(path_params, dict):
            for k, v in path_params.items():
                body.setdefault(k, v)

        return body

    # Caso Step Functions u otro que mande un dict simple
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
    id_pedido = event.get("id_pedido") or event.get("id")

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


# ------------------------- Lambda 1: pagado -> cocina ------------------------- #

def pagado_a_cocina(event, context):
    """
    Transición:
      pagado -> cocina
      - Crea registro en COCINA (status = 'cocinando')
      - Actualiza PEDIDOS.estado_pedido = 'cocina'
      - Si viene de Step Functions, guarda task_token_cocina
    """
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "pagado")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]
    id_empleado = event.get("id_empleado")
    task_token = event.get("taskToken")

    # 1) Crear registro en COCINA
    item_cocina = {
        "id_pedido": id_pedido,
        "id_empleado": id_empleado or "no_asignado",
        "hora_comienzo": obtener_timestamp_iso(),
        "hora_fin": None,
        "status": "cocinando"
    }
    tabla_cocina.put_item(Item=item_cocina)

    # 2) Actualizar estado del pedido a 'cocina' + token si aplica
    update_expr = "SET estado_pedido = :e"
    expr_values = {":e": "cocina"}

    if task_token:
        update_expr += ", task_token_cocina = :t"
        expr_values[":t"] = task_token

    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición pagado -> cocina realizada (esperando confirmación de cocina si viene de Step Functions)",
            "pedido": {
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            },
            "registro_cocina": item_cocina,
            "taskToken_guardado": bool(task_token)
        })
    }


# ------------------------- Lambda 2: cocina -> empaquetamiento ------------------------- #

def cocina_a_empaquetamiento(event, context):
    """
    Transición:
      cocina -> empaquetamiento
      - Actualiza COCINA (status = 'terminado')
      - Crea registro en DESPACHADOR (empaquetamiento, status='cocinando')
      - Actualiza PEDIDOS.estado_pedido = 'empaquetamiento'
      - Guarda task_token_empaquetamiento si viene de Step Functions
    """
    event = parse_event(event)

    pedido, error = validar_pedido_y_estado(event, "cocina")
    if error:
        return error

    tenant_id = pedido["tenant_id"]
    id_pedido = pedido["id"]
    id_empleado_despachador = event.get("id_empleado")
    task_token = event.get("taskToken")

    # 1) Terminar COCINA
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
        "status": "cocinando"  # puedes cambiar el texto a 'empaquetando' si quieres
    }
    tabla_despachador.put_item(Item=item_despachador)

    # 3) Actualizar estado del pedido a 'empaquetamiento' + token
    update_expr = "SET estado_pedido = :e"
    expr_values = {":e": "empaquetamiento"}

    if task_token:
        update_expr += ", task_token_empaquetamiento = :t"
        expr_values[":t"] = task_token

    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición cocina -> empaquetamiento realizada (esperando confirmación de empaquetamiento si viene de Step Functions)",
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
            },
            "taskToken_guardado": bool(task_token)
        })
    }


# ------------------------- Lambda 3: empaquetamiento -> delivery ------------------------- #

def empaquetamiento_a_delivery(event, context):
    """
    Transición:
      empaquetamiento -> delivery
      - Actualiza DESPACHADOR (status = 'terminado')
      - Crea registro en DELIVERY (status='en camino')
      - Actualiza PEDIDOS.estado_pedido = 'delivery'
      - Guarda task_token_delivery si viene de Step Functions
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
    task_token = event.get("taskToken")

    # 1) Terminar empaquetamiento (DESPACHADOR)
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

    # 3) Actualizar estado del pedido a 'delivery' + token
    update_expr = "SET estado_pedido = :e"
    expr_values = {":e": "delivery"}

    if task_token:
        update_expr += ", task_token_delivery = :t"
        expr_values[":t"] = task_token

    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "Transición empaquetamiento -> delivery realizada (esperando confirmación de entrega si viene de Step Functions)",
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
            },
            "taskToken_guardado": bool(task_token)
        })
    }


# ------------------------- Lambda 4: delivery -> entregado ------------------------- #

def delivery_a_entregado(event, context):
    """
    Transición final:
      delivery -> entregado
      - Actualiza DELIVERY.status = 'cumplido'
      - Actualiza PEDIDOS.estado_pedido = 'entregado'
      (se ejecuta automáticamente cuando Step Functions pasa a este estado,
       después de que confirmes 'delivery-entregado' vía confirmar_paso)
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
    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression="SET estado_pedido = :e",
        ExpressionAttributeValues={":e": "entregado"}
    )

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


# ------------------------- Lambda de callback: confirmar_paso ------------------------- #

def confirmar_paso(event, context):
    """
    Lambda de callback para avanzar el Step Function.
    Espera un body JSON con:
      - tenant_id
      - id_pedido
      - paso: 'cocina-lista' | 'empaquetamiento-listo' | 'delivery-entregado'
    """
    event = parse_event(event)

    tenant_id = event.get("tenant_id")
    id_pedido = event.get("id_pedido") or event.get("id")
    paso = event.get("paso")

    if not tenant_id or not id_pedido or not paso:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Faltan tenant_id, id_pedido o paso"
            })
        }

    resp = tabla_pedidos.get_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        }
    )
    pedido = resp.get("Item")
    if not pedido:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "mensaje": "Pedido no encontrado",
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            })
        }

    # Mapeo paso -> nombre del campo del token
    mapa_paso_token = {
        "cocina-lista": "task_token_cocina",
        "empaquetamiento-listo": "task_token_empaquetamiento",
        "delivery-entregado": "task_token_delivery"
    }

    nombre_campo = mapa_paso_token.get(paso)
    if not nombre_campo:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": f"Paso '{paso}' no soportado. "
                           f"Usa uno de: {list(mapa_paso_token.keys())}"
            })
        }

    task_token = pedido.get(nombre_campo)
    if not task_token:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": f"No se encontró {nombre_campo} para este pedido. "
                           f"¿Se inició el flujo correctamente?"
            })
        }

    # Enviamos el callback a Step Functions
    stepfunctions_client.send_task_success(
        TaskToken=task_token,
        output=json.dumps({
            "mensaje": f"Confirmación de paso '{paso}'",
            "tenant_id": tenant_id,
            "id_pedido": id_pedido
        })
    )

    # Limpiamos el token del pedido
    tabla_pedidos.update_item(
        Key={
            "tenant_id": tenant_id,
            "id": id_pedido
        },
        UpdateExpression=f"REMOVE {nombre_campo}"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": f"Confirmación '{paso}' enviada a Step Functions",
            "tenant_id": tenant_id,
            "id_pedido": id_pedido
        })
    }

