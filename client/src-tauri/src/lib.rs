use tauri::Manager;

#[tauri::command]
fn get_server_url(app: tauri::AppHandle) -> String {
    let store = app.state::<tauri_plugin_store::StoreCollection<tauri::Wry>>();
    tauri_plugin_store::with_store(app.clone(), store, "settings.json", |s| {
        Ok(s.get("server_url")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "http://localhost:3000".to_string()))
    })
    .unwrap_or_else(|_| "http://localhost:3000".to_string())
}

#[tauri::command]
fn set_server_url(app: tauri::AppHandle, url: String) -> Result<(), String> {
    let store = app.state::<tauri_plugin_store::StoreCollection<tauri::Wry>>();
    tauri_plugin_store::with_store(app.clone(), store, "settings.json", |s| {
        s.insert("server_url".to_string(), serde_json::json!(url))?;
        s.save()
    })
    .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .invoke_handler(tauri::generate_handler![get_server_url, set_server_url])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
