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
    - SQS: event["Records"][0]["body"] con JSON
    - Step Functions con waitForTaskToken: { "taskToken": "...", "input": {...} }
    - HTTP API (GET y POST)
    - Step Functions normal: input directo
    """
    # --- Evento desde SQS ---
    if "Records" in event and isinstance(event["Records"], list) and event["Records"]:
        record = event["Records"][0]
        if record.get("eventSource") == "aws:sqs":
            body_str = record.get("body", "{}")
            try:
                body = json.loads(body_str)
            except Exception:
                body = {"raw_body": body_str}
            return body

    # --- STEP FUNCTIONS waitForTaskToken ---
    if "taskToken" in event and "input" in event and isinstance(event["input"], dict):
        base = event["input"].copy()
        base["taskToken"] = event["taskToken"]
        return base

    # ------------------------------------------------------------------
    # 🟦 NUEVO: NORMALIZAR HTTP API (GET o POST)
    # ------------------------------------------------------------------
    if event.get("version") == "2.0":  # HTTP API siempre tiene version 2.0
        result = {}

        # Query params
        if event.get("queryStringParameters"):
            for k, v in event["queryStringParameters"].items():
                result[k] = v

        # Path params
        if event.get("pathParameters"):
            for k, v in event["pathParameters"].items():
                result[k] = v

        # Body si existe (POST)
        body = event.get("body")
        if body:
            try:
                body_data = json.loads(body)
                if isinstance(body_data, dict):
                    for k, v in body_data.items():
                        result[k] = v
            except:
                pass

        return result
    # ------------------------------------------------------------------

    # --- Caso Step Functions u otro que mande un dict simple ---
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


def obtener_pedido(event, context):
    """
    GET /pedidos/{id_pedido}?tenant_id=TENANT
    Devuelve datos completos del pedido + cocina + empaquetamiento + delivery.
    """

    print("DEBUG obtener_pedido raw event:", json.dumps(event))

    event = parse_event(event)

    id_pedido = event.get("id_pedido") or event.get("path_id_pedido")
    tenant_id = event.get("tenant_id")

    if not id_pedido or not tenant_id:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Falta tenant_id o id_pedido"
            })
        }

    # 1. Obtener PEDIDO
    try:
        pedido_resp = tabla_pedidos.get_item(
            Key={"tenant_id": tenant_id, "id": id_pedido}
        )
        pedido = pedido_resp.get("Item")
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"mensaje": "Error leyendo pedido", "detalle": str(e)})
        }

    if not pedido:
        return {
            "statusCode": 404,
            "body": json.dumps({"mensaje": "Pedido no encontrado"})
        }

    # 2. Obtener COCINA
    cocina_resp = tabla_cocina.get_item(Key={"id_pedido": id_pedido})
    cocina = cocina_resp.get("Item", {})

    # 3. Obtener EMPAQUETAMIENTO
    des_resp = tabla_despachador.get_item(Key={"id_pedido": id_pedido})
    despachador = des_resp.get("Item", {})

    # 4. Obtener DELIVERY
    delivery_resp = tabla_delivery.get_item(Key={"id_pedido": id_pedido})
    delivery = delivery_resp.get("Item", {})

    # Respuesta
    return {
        "statusCode": 200,
        "body": json.dumps({
            "pedido": pedido,
            "cocina": cocina,
            "empaquetamiento": despachador,
            "delivery": delivery
        })
    }


from boto3.dynamodb.conditions import Key, Attr

def listar_pedidos(event, context):
    """
    GET /pedidos?tenant_id=X&estado=cocina,delivery
    Devuelve todos los pedidos de un tenant, filtrando por uno o varios estados.
    """

    print("DEBUG listar_pedidos raw event:", json.dumps(event))

    event = parse_event(event)

    tenant_id = event.get("tenant_id")
    estados_raw = event.get("estado")  # puede ser "cocina", "cocina,delivery", etc.

    if not tenant_id or not estados_raw:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Debe enviar tenant_id y estado (uno o varios separados por coma)."
            })
        }

    # Procesar lista de estados
    lista_estados = [e.strip() for e in estados_raw.split(",") if e.strip()]

    print("DEBUG lista_estados:", lista_estados)

    pedidos_finales = []

    try:
        # Query por tenant_id
        resp = tabla_pedidos.query(
            KeyConditionExpression=Key("tenant_id").eq(tenant_id)
        )
        items = resp.get("Items", [])

        # Filtrar manualmente por los estados solicitados
        for item in items:
            if item.get("estado_pedido") in lista_estados:
                pedidos_finales.append(item)

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "mensaje": "Error consultando pedidos",
                "detalle": str(e)
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "tenant_id": tenant_id,
            "filtro_estados": lista_estados,
            "cantidad": len(pedidos_finales),
            "pedidos": pedidos_finales
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
      - OPCIONAL: id_empleado, repartidor, id_repartidor, origen, destino
    """
    # Para ver exactamente qué llega desde API Gateway
    print("DEBUG raw event:", json.dumps(event))

    event = parse_event(event)
    print("DEBUG parsed event:", json.dumps(event))

    tenant_id = event.get("tenant_id")
    id_pedido = event.get("id_pedido") or event.get("id")
    paso = event.get("paso")

    id_empleado = event.get("id_empleado")
    repartidor = event.get("repartidor")
    id_repartidor = event.get("id_repartidor")
    origen = event.get("origen")
    destino = event.get("destino")

    if not tenant_id or not id_pedido or not paso:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Faltan tenant_id, id_pedido o paso",
                "event": event
            })
        }

    # 1) Leer pedido de Dynamo
    try:
        resp = tabla_pedidos.get_item(
            Key={
                "tenant_id": tenant_id,
                "id": id_pedido
            }
        )
    except Exception as e:
        print("ERROR get_item:", repr(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "mensaje": "Error al leer pedido en DynamoDB",
                "detalle": str(e)
            })
        }

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
                "mensaje": f"Paso '{paso}' no soportado. Usa uno de: {list(mapa_paso_token.keys())}"
            })
        }

    task_token = pedido.get(nombre_campo)
    if not task_token:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": f"No se encontró {nombre_campo} para este pedido. "
                           f"¿Seguro que la ejecución del Step Function está esperando en este paso?"
            })
        }

    # 2) Actualizar info opcional (empleado / repartidor) y enviar callback
    try:
        # 2.a) Actualizar COCINA / DESPACHADOR / DELIVERY según el paso
        if paso == "cocina-lista" and id_empleado:
            tabla_cocina.update_item(
                Key={"id_pedido": id_pedido},
                UpdateExpression="SET id_empleado = :e",
                ExpressionAttributeValues={":e": id_empleado}
            )

        elif paso == "empaquetamiento-listo" and id_empleado:
            tabla_despachador.update_item(
                Key={"id_pedido": id_pedido},
                UpdateExpression="SET id_empleado = :e",
                ExpressionAttributeValues={":e": id_empleado}
            )

        elif paso == "delivery-entregado":
            update_expr = []
            expr_vals = {}

            if repartidor:
                update_expr.append("repartidor = :r")
                expr_vals[":r"] = repartidor
            if id_repartidor:
                update_expr.append("id_repartidor = :ir")
                expr_vals[":ir"] = id_repartidor
            if origen:
                update_expr.append("origen = :o")
                expr_vals[":o"] = origen
            if destino:
                update_expr.append("destino = :d")
                expr_vals[":d"] = destino

            if update_expr:
                tabla_delivery.update_item(
                    Key={"id_pedido": id_pedido},
                    UpdateExpression="SET " + ", ".join(update_expr),
                    ExpressionAttributeValues=expr_vals
                )

        # 2.b) Enviar callback a Step Functions
        resp_sf = stepfunctions_client.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "tenant_id": tenant_id,
                "id_pedido": id_pedido,
                "paso_confirmado": paso
            })
        )
        print("DEBUG send_task_success resp:", resp_sf)

    except Exception as e:
        print("ERROR send_task_success o update:", repr(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "mensaje": "Error al actualizar datos o enviar confirmación a Step Functions",
                "detalle": str(e)
            })
        }

    # 3) Limpiar el token del pedido
    try:
        tabla_pedidos.update_item(
            Key={
                "tenant_id": tenant_id,
                "id": id_pedido
            },
            UpdateExpression=f"REMOVE {nombre_campo}"
        )
    except Exception as e:
        print("ERROR update_item REMOVE token:", repr(e))
        return {
            "statusCode": 200,
            "body": json.dumps({
                "mensaje": f"Confirmación '{paso}' enviada a Step Functions, "
                           f"pero hubo un error al limpiar el token en DynamoDB",
                "detalle": str(e),
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
                "mensaje": f"Confirmación '{paso}' enviada a Step Functions",
                "tenant_id": tenant_id,
                "id_pedido": id_pedido
        })
    }





