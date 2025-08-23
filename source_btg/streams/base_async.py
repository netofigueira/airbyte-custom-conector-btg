import random
import time
import requests
import json
import itertools
from io import BytesIO
from zipfile import ZipFile
from typing import Iterable, Mapping, List, Any, Optional, Union
from datetime import datetime, timedelta

from airbyte_cdk.sources.streams.http import HttpStream


class AsyncJobStream(HttpStream):
    """
    Fluxo assíncrono do BTG:
      1) submit (POST/GET) -> ticketId
      2) polling (GET /reports/Ticket?ticketId=...) -> XML/ZIP inline (ou JSON com links)
      3) parse do payload em registros
    """

    def __init__(self, config: Mapping[str, Any], token_provider, route: Mapping[str, Any]):
        self.cfg = config
        self.route = route
        self._token_provider = token_provider
        self._name = route.get("name", "btg_stream")
        # Inicializar session do requests
        self.session = requests.Session()
        super().__init__()


    def _timeout(self) -> int:
        return int(self.cfg.get("http_timeout_seconds")
                or self.cfg.get("technical", {}).get("timeout_seconds")
                or 60)

    def _max_wait(self) -> int:
        return int(self.cfg.get("max_wait_seconds") or 900)

    def _uses_date(self) -> bool:
        submit_body = self.route.get("submit_body", {})
        submit_params = self.route.get("submit_params", {})
        blob = json.dumps({"b": submit_body, "p": submit_params}, ensure_ascii=False).lower()
        return ("{{date_iso}}" in blob) or ("{{date}}" in blob) or ("{{date_str}}" in blob)
    # ========== stubs obrigatórios do CDK ==========
    @property
    def url_base(self) -> str:
        base = (self.cfg.get("base_url") or self.cfg.get("url_base") or "https://funds.btgpactual.com").rstrip("/")
        return base + "/"

    def path(self, **kwargs) -> str:
        return self.route.get("submit_path", "/")

    def parse_response(self, response, **kwargs) -> Iterable[Mapping[str, Any]]:
        return []

    @property
    def primary_key(self) -> Optional[Union[str, List[str]]]:
        return ["_ticket_id", "_row_number"]

    def next_page_token(self, response, **kwargs):
        return None
    # ===============================================

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str):
        self._name = value


    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def cursor_field(self) -> str:
        # você já emite esse campo nos yields
        return "_dt_referencia"

    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]):
        state = dict(current_stream_state or {})
        key = self.route.get("name", self._name)
        cur = latest_record.get("_dt_referencia")
        last = state.get(key)
        if cur and (not last or cur > last):
            state[key] = cur
        return state


    # token provider
    @property
    def tk(self):
        return self._token_provider

    # ---------- headers ----------
    def _hdr(self, kind: str) -> Mapping[str, str]:
        token = self.tk.get()
        base = {"Accept": "*/*"}
        if kind == "bearer":
            base["Authorization"] = f"Bearer {token}"
        else:
            base["X-SecureConnect-Token"] = token
        return base

    # ---------- utils para templates ----------
    def expand_templates(self, template: Any, context: Mapping) -> Any:
        """Substitui placeholders tipo {{persona}} nos templates"""
        if isinstance(template, str):
            for key, value in context.items():
                template = template.replace(f"{{{{{key}}}}}", str(value))
            return template
        elif isinstance(template, dict):
            return {k: self.expand_templates(v, context) for k, v in template.items()}
        elif isinstance(template, list):
            return [self.expand_templates(item, context) for item in template]
        return template

    def dot_get(self, data: dict, path: str, default=None):
        """Pega valor aninhado tipo 'result.ticketId'"""
        keys = path.split('.')
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    # ---------- daterange helper ----------
    def daterange(self, start_date: str, end_date: str = None, step_days: int = 1):
        """Gera range de datas"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        
        current = start
        while current <= end:
            yield current
            current += timedelta(days=step_days)

    def stream_slices(self, *, sync_mode, cursor_field=None, stream_state=None, **kwargs):
        route_name = self.route.get("name", "")
        endpoint = "_".join(route_name.split("_")[1:]) if "_" in route_name else route_name

        uses_date = self._uses_date()
        sync_config = self.cfg.get("sync_schedule", {}) or {}
        start_date = sync_config.get("start_date")
        end_date = sync_config.get("end_date")
        step = int(sync_config.get("date_step_days", 1))

        windows = [None]
        if uses_date and start_date and end_date:
            windows = [{"date_str": d.strftime("%d/%m/%Y"),
                        "date_iso": d.strftime("%Y-%m-%d")}
                    for d in self.daterange(start_date, end_date, step)]

        endpoint_params = self.cfg.get("endpoint_params")
        if endpoint_params is None:
            endpoint_params = self._get_endpoint_parameters(endpoint)

        combos = self._generate_param_combinations(endpoint_params)

        for w in windows:
            base_slice = w or {}
            if combos:
                for params in combos:
                    yield {**base_slice, **params}
            else:
                yield base_slice


    def _get_endpoint_parameters(self, endpoint: str) -> dict:
        endpoints_config = self.cfg.get("endpoints") or {}
        endpoint_config = endpoints_config.get(endpoint) or {}
        return endpoint_config.get("params") or {}

    def _generate_param_combinations(self, params: dict) -> list:
        if not params:
            return []
        keys = list(params.keys())
        vals = []
        for k in keys:
            v = params[k]
            if isinstance(v, list):
                vals.append(v if v else [None])
            else:
                vals.append([v])
        return [dict(zip(keys, combo)) for combo in itertools.product(*vals)]

    # ---------- submit: retorna ticketId ----------
    def _submit(self, slice_ctx: Mapping) -> str:
        method = self.route.get("submit_method", "POST").upper()
        path = self.route.get("submit_path", "/")
        auth = self.route.get("submit_auth", "bearer")
        body = self.expand_templates(self.route.get("submit_body", {}), slice_ctx)
        params = self.expand_templates(self.route.get("submit_params", {}), slice_ctx)
        url = self.url_base.rstrip("/") + "/" + path.lstrip("/")

        # Debug
        print(f"DEBUG _submit: {method} {url}")
        print(f"DEBUG body: {body}")
        print(f"DEBUG params: {params}")
        print(f"DEBUG slice_ctx: {slice_ctx}")
        print(f"DEBUG auth type: {auth}")

        # Pegar token fresco
        token = self.tk.get()
        print(f"DEBUG token: {token[:30]}..." if token else "DEBUG token: None")

        # Headers baseado no tipo de auth
        if auth == "bearer":
            headers = {
                "Accept": "*/*",
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        elif auth == "xsecure":
            headers = {
                "Accept": "*/*",
                "X-SecureConnect-Token": token,
                "Content-Type": "application/json"
            }
        else:
            # Default bearer
            headers = {
                "Accept": "*/*", 
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

        print(f"DEBUG headers: {headers}")

        r = self.session.request(
            method,
            url,
            json=body if method == "POST" else None,
            params=params if method == "GET" else None,
            headers=headers,
            timeout=self.cfg.get("http_timeout_seconds", 60),
        )
        
        print(f"DEBUG response status: {r.status_code}")
        print(f"DEBUG response: {r.text}")
        
        r.raise_for_status()
        js = r.json()
        
        # Procurar ticket ID em vários lugares possíveis
        ticket = (js.get("ticketId") or 
                 self.dot_get(js, "result.ticketId") or 
                 self.dot_get(js, "ticket.id") or 
                 js.get("id") or
                 js.get("ticket"))
                 
        if not ticket:
            raise Exception(f"Submit sem ticket. Response: {js}")
        return str(ticket)

    # ---------- polling: Ticket -> XML/ZIP inline (ou JSON) ----------
    def _wait_ticket(self, ticket_id: str) -> Mapping:
        path = self.route.get("ticket_path", "/reports/Ticket")
        auth = self.route.get("ticket_auth", "xsecure")
        url = self.url_base.rstrip("/") + "/" + path.lstrip("/")

        deadline = time.time() + int(self.cfg.get("max_wait_seconds", 900))
        delay = 5

        print(f"DEBUG _wait_ticket: polling {ticket_id}")

        while True:
            r = self.session.get(
                url,
                params={"ticketId": ticket_id},
                headers=self._hdr(auth),
                timeout=self.cfg.get("http_timeout_seconds", 60),
            )
            
            print(f"DEBUG poll status: {r.status_code}")
            
            ctype = (r.headers.get("Content-Type") or "").lower()
            body = r.content

            if r.status_code == 200:
                looks_xml = body.lstrip().startswith(b"<")
                looks_zip = len(body) >= 2 and body[0:2] == b"PK"

                # Conteúdo inline (XML/ZIP direto)
                if "xml" in ctype or "text/" in ctype or looks_xml or looks_zip:
                    print(f"DEBUG: Got inline content ({len(body)} bytes)")
                    return {"__mode__": "inline", "payload": body}

                # Resposta JSON
                if "json" in ctype:
                    try:
                        js = r.json()
                        print(f"DEBUG: Got JSON response: {js}")
                        
                        # Verificar se ainda está processando
                        result = js.get("result", "")
                        if result in ["Processando", "Processing", "In Progress", "PROCESSING", "PENDING"]:
                            print(f"DEBUG: Still processing ({result})... waiting")
                            # Continuar o loop
                        else:
                            # Job completou
                            
                            # Se tem arquivos para download
                            if js.get("files"):
                                print(f"DEBUG: Found files for download: {js.get('files')}")
                                return {"__mode__": "download", "json": js}
                            
                            # Se o result contém dados diretos
                            result_field = self.route.get("ticket_result_field", "result")
                            ready = self.dot_get(js, result_field) if result_field else js
                            
                            if ready and ready not in ["Processando", "Processing", "In Progress", "PROCESSING", "PENDING"]:
                                # Se o result é XML como string
                                if isinstance(ready, str) and ready.lstrip().startswith("<"):
                                    return {"__mode__": "inline", "payload": ready.encode("utf-8")}
                                # Retorna os dados JSON
                                return {"__mode__": "json", "json": js}
                                
                        # Se chegou aqui, ainda processando ou sem dados válidos
                        
                    except Exception as e:
                        print(f"DEBUG: Error parsing JSON: {e}")
                        pass

            # Timeout check
            if time.time() > deadline:
                raise Exception(f"Timeout aguardando ticket {ticket_id}")

            print(f"DEBUG: Waiting {delay}s...")
            time.sleep(delay + random.random() * 2)
            delay = min(delay * 1.5, 45)

    # ---------- download (quando JSON traz URL) ----------
    def _download(self, url_or_path: str) -> bytes:
        auth = self.route.get("download_auth", "xsecure")
        url = (url_or_path if url_or_path.startswith(("http://", "https://")) 
               else self.url_base.rstrip("/") + "/" + url_or_path.lstrip("/"))
        
        print(f"DEBUG: Downloading from {url}")
        r = self.session.get(
            url,
            headers=self._hdr(auth),
            timeout=max(120, self.cfg.get("http_timeout_seconds", 60)),
        )
        r.raise_for_status()
        return r.content

    # ---------- unzip se necessário ----------
    @staticmethod
    def _unzip_if_needed(raw: bytes) -> bytes:
        if len(raw) >= 2 and raw[0:2] == b"PK":
            print(f"DEBUG: Unzipping content ({len(raw)} bytes)")
            with ZipFile(BytesIO(raw)) as zf:
                first = zf.namelist()[0]
                print(f"DEBUG: Extracting {first}")
                return zf.read(first)
        return raw

    # ---------- parse melhorado ----------
    def _parse(self, payload: bytes) -> List[Mapping]:
        """Parse melhorado com suporte para XML, CSV e JSON"""
        try:
            text = payload.decode('utf-8')
            text_stripped = text.strip()
            
            # JSON
            if text_stripped.startswith('{') or text_stripped.startswith('['):
                data = json.loads(text_stripped)
                if isinstance(data, list):
                    return data
                return [data]
            
            # XML (básico)
            if text_stripped.startswith('<'):
                # Parse XML simples - você pode melhorar com xml.etree
                import xml.etree.ElementTree as ET
                try:
                    root = ET.fromstring(text_stripped)
                    # Converte XML em dict básico
                    def xml_to_dict(element):
                        result = {}
                        if element.text and element.text.strip():
                            result['text'] = element.text.strip()
                        for child in element:
                            child_data = xml_to_dict(child)
                            if child.tag in result:
                                if not isinstance(result[child.tag], list):
                                    result[child.tag] = [result[child.tag]]
                                result[child.tag].append(child_data)
                            else:
                                result[child.tag] = child_data
                        result.update(element.attrib)
                        return result
                    
                    parsed = xml_to_dict(root)
                    return [parsed] if parsed else [{"xml_content": text_stripped}]
                except ET.ParseError:
                    return [{"xml_content": text_stripped}]
            
            # CSV (básico)
            if '\n' in text_stripped and (',' in text_stripped or ';' in text_stripped):
                lines = text_stripped.split('\n')
                if len(lines) > 1:
                    # Detectar separador
                    sep = ',' if ',' in lines[0] else ';'
                    headers = [h.strip() for h in lines[0].split(sep)]
                    rows = []
                    for line in lines[1:]:
                        if line.strip():
                            values = [v.strip() for v in line.split(sep)]
                            if len(values) == len(headers):
                                rows.append(dict(zip(headers, values)))
                    return rows if rows else [{"csv_content": text_stripped}]
            
            # Texto simples
            return [{"raw_content": text_stripped}]
            
        except Exception as e:
            print(f"DEBUG: Parse error: {e}")
            return [{"raw_content": payload.decode('utf-8', errors='ignore'), "parse_error": str(e)}]

    def get_json_schema(self):
        return {"type": "object", "additionalProperties": True}

    # ---------- loop principal ----------
    def read_records(self, stream_slice: Mapping = None, **kwargs) -> Iterable[Mapping]:
        print(f"🚨 DEBUG read_records: ENTRADA")
        print(f"🚨 DEBUG read_records: stream_slice = {stream_slice}")
        print(f"🚨 DEBUG read_records: type(stream_slice) = {type(stream_slice)}")
        print(f"🚨 DEBUG read_records: kwargs = {kwargs}")
        
        slice_ = stream_slice or {}
        slice_ctx = {
            "date": slice_.get("date_str"),
            "date_str": slice_.get("date_str"),
            "date_iso": slice_.get("date_iso"),
        }
        
        # Adicionar todos os parâmetros extras do slice
        for key, value in slice_.items():
            if key not in [ "date_str", "date_iso"]:
                slice_ctx[key] = value
        
        print(f"🔍 DEBUG read_records: slice_ctx final = {slice_ctx}")

        try:
            # 1. Submit job
            ticket = self._submit(slice_ctx)
            print(f"DEBUG: Got ticket {ticket}")
            
            # 2. Wait for completion
            status = self._wait_ticket(ticket)
            print(f"DEBUG: Ticket ready, mode: {status.get('__mode__')}")
            
            row_idx = 0

            # 3. Process result
            if status.get("__mode__") == "inline":
                # Conteúdo direto (XML/ZIP)
                payload = self._unzip_if_needed(status["payload"])
                rows = self._parse(payload)
                
                for rec in rows:
                    yield {
                        **(rec if isinstance(rec, dict) else {"value": rec}),
                        "_route": self._name,
                        "_dt_referencia": slice_ctx["date"],
                        "_ticket_id": ticket,
                        "_row_number": row_idx,
                    }
                    row_idx += 1
                    
            elif status.get("__mode__") == "download":
                # JSON com arquivos para download
                json_data = status["json"]
                files = json_data.get("files", [])
                
                for file_info in files:
                    try:
                        # file_info pode ser string (URL) ou dict
                        if isinstance(file_info, str):
                            url = file_info
                            file_meta = {"url": url}
                        elif isinstance(file_info, dict):
                            url = file_info.get("url") or file_info.get("path") or file_info.get("link")
                            file_meta = file_info
                        else:
                            continue
                            
                        if not url:
                            continue
                            
                        payload = self._download(url)
                        payload = self._unzip_if_needed(payload)
                        rows = self._parse(payload)
                        
                        for rec in rows:
                            yield {
                                **(rec if isinstance(rec, dict) else {"value": rec}),
                                "_route": self._name,
                                "_dt_referencia": slice_ctx["date"],
                                "_ticket_id": ticket,
                                "_row_number": row_idx,
                                "_file_info": file_meta,
                            }
                            row_idx += 1
                            
                    except Exception as e:
                        print(f"ERROR downloading file {file_info}: {e}")
                        yield {
                            "error": f"Download failed: {e}",
                            "file_info": file_info,
                            "_route": self._name,
                            "_dt_referencia": slice_ctx["date"],
                            "_ticket_id": ticket,
                            "_row_number": row_idx,
                        }
                        row_idx += 1
                        
            elif status.get("__mode__") == "json":
                # Dados JSON diretos
                json_data = status["json"]
                result_field = self.route.get("ticket_result_field", "result")
                result_data = self.dot_get(json_data, result_field) if result_field else json_data
                
                if result_data and result_data not in ["Processando", "Processing", "In Progress", "PROCESSING", "PENDING"]:
                    # Se result_data é uma lista
                    if isinstance(result_data, list):
                        rows = result_data
                    elif isinstance(result_data, dict):
                        rows = [result_data]
                    else:
                        rows = [{"value": result_data}]
                        
                    for rec in rows:
                        yield {
                            **(rec if isinstance(rec, dict) else {"value": rec}),
                            "_route": self._name,
                            "_dt_referencia": slice_ctx["date"],
                            "_ticket_id": ticket,
                            "_row_number": row_idx,
                            "_source_json": json_data,
                        }
                        row_idx += 1
                else:
                    yield {
                        "message": f"No processable data found in JSON response",
                        "json_response": json_data,
                        "_route": self._name,
                        "_dt_referencia": slice_ctx["date"],
                        "_ticket_id": ticket,
                        "_row_number": 0,
                    }
            else:
                # Modo desconhecido
                yield {
                    "error": f"Unknown response mode: {status.get('__mode__')}",
                    "status": status,
                    "_route": self._name,
                    "_dt_referencia": slice_ctx["date"],
                    "_ticket_id": ticket,
                    "_row_number": 0,
                }
                
        except Exception as e:
            print(f"ERROR in read_records: {e}")
            # Yield erro como record para debug
            yield {
                "error": str(e),
                "slice_ctx": slice_ctx,
                "_route": self._name,
                "_dt_referencia": slice_ctx.get("date"),
                "_ticket_id": "error",
                "_row_number": 0,
            }


# Alias para compatibilidade
JobRouteGeneric = AsyncJobStream

__all__ = ["AsyncJobStream", "JobRouteGeneric"]