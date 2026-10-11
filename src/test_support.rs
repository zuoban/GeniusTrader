use axum::Router;

pub async fn mock_server(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local mock server requires loopback network permission");
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), task)
}

/// Older account fixtures have no conditional orders. Preserve their original
/// fallbacks and failure injection for all existing endpoints.
pub async fn mock_server_with_algos(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    mock_server(app.route(
        "/fapi/v1/openAlgoOrders",
        axum::routing::get(|| async { axum::Json(serde_json::json!([])) }),
    ))
    .await
}

pub async fn mock_server_with_protection(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    use axum::{Json, extract::Query, routing::get};
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    let stops = Arc::new(Mutex::new(BTreeMap::<String, Value>::new()));
    let read = stops.clone();
    let post = stops.clone();
    let cancel = stops.clone();
    let app=app.route("/fapi/v1/openAlgoOrders", get(move || {let s=stops.clone();async move {
        Json(s.lock().unwrap().values().filter(|v|v["algoStatus"]=="NEW").cloned().collect::<Vec<_>>())
    }})).route("/fapi/v1/algoOrder", get(move |Query(q):Query<BTreeMap<String,String>>| {
        let s=read.clone();async move {Json(s.lock().unwrap().values().find(|v|
            q.get("clientAlgoId").is_some_and(|id|Some(id.as_str())==v["clientAlgoId"].as_str())
            || q.get("algoId").is_some_and(|id|id==&v["algoId"].to_string())).unwrap().clone())}
    }).post(move |body:String| {let s=post.clone();async move {
        let q:BTreeMap<_,_>=url::form_urlencoded::parse(body.as_bytes()).into_owned().collect();
        let mut s=s.lock().unwrap();let v=json!({"algoId":s.len()+1,"clientAlgoId":q["clientAlgoId"],
            "symbol":q["symbol"],"side":q["side"],"positionSide":"BOTH","orderType":"STOP_MARKET",
            "quantity":q["quantity"],"triggerPrice":q["triggerPrice"],"algoStatus":"NEW",
            "workingType":"CONTRACT_PRICE","reduceOnly":true,"closePosition":false,"priceProtect":false,"actualOrderId":""});
        s.insert(q["clientAlgoId"].clone(),v.clone());Json(v)
    }}).delete(move |Query(q):Query<BTreeMap<String,String>>| {let s=cancel.clone();async move {
        let mut s=s.lock().unwrap();let v=s.values_mut().find(|v|
            q.get("algoId").is_some_and(|id|id==&v["algoId"].to_string())).unwrap();
        v["algoStatus"]=json!("CANCELED");Json(json!({"algoId":v["algoId"],"clientAlgoId":v["clientAlgoId"],"code":"200"}))
    }}));
    mock_server(app).await
}
