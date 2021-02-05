use futures::StreamExt;
use tokio::sync::mpsc;
use vector::{
    transforms::{FunctionTransform, TaskTransform},
    Event,
};

pub async fn drive_function_transform(
    mut component: Box<dyn FunctionTransform>,
    mut in_rx: mpsc::Receiver<Event>,
    mut out_tx: mpsc::Sender<Vec<Event>>,
) {
    while let Some(in_event) = in_rx.recv().await {
        let mut out_events = Vec::new();
        component.transform(&mut out_events, in_event);
        out_tx.send(out_events).await.unwrap();
    }
}

pub async fn drive_task_transform(
    component: Box<dyn TaskTransform>,
    in_rx: mpsc::Receiver<Event>,
    mut out_tx: mpsc::Sender<Event>,
) {
    let mut out_stream = component.transform(Box::pin(in_rx));
    while let Some(event) = out_stream.next().await {
        out_tx.send(event).await.unwrap();
    }
}
