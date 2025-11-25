import json
import pickle
import wizata_dsapi
import pickle
from sklearn.linear_model import LinearRegression

def train_model():
    df = wizata_dsapi.api().query(
        datapoints=["mt1_bearing1", "mt1_bearing2", "mt1_bearing3", "mt1_bearing4"],
        agg_method="mean",
        interval=300000,
        start="now-1d",
        end="now"
    )
    x = df[['mt1_bearing1', 'mt1_bearing2','mt1_bearing3']]
    y = df['mt1_bearing4']
    my_model = LinearRegression()
    my_model.fit(x, y)
    return my_model

def save_locally_pickle(my_model):
    with open('model.pkl', 'wb') as f:
        pickle.dump(my_model, f)

def upload_model_from_pickle():
    with open("model.pkl", "rb") as f:
        my_model = pickle.load(f)

    model_info = wizata_dsapi.api().upload_model(
        model_info=wizata_dsapi.ModelInfo(
            key="models-01-sample-docs-pickle",
            twin_hardware_id="mef_plant_a",
            trained_model=my_model,
        )
    )
    print(model_info.identifier(include_alias=True))
    return model_info.identifier(include_alias=True)

def upload_model_from_pickle_as_bytes():
    with open("model.pkl", "rb") as f:
        my_model_bytes = f.read()

    model_info = wizata_dsapi.api().upload_model(
        model_info=wizata_dsapi.ModelInfo(
            key="models-01-sample-docs-pickle",
            twin_hardware_id="mef_plant_a",
        ),
        bytes_content=my_model_bytes
    )
    print(model_info.identifier(include_alias=True))
    return model_info.identifier(include_alias=True)

def upload_model(my_model):
    model_info = wizata_dsapi.api().upload_model(
        model_info=wizata_dsapi.ModelInfo(
            key="models-01-sample-docs",
            twin_hardware_id="mef_plant_a",
            trained_model=my_model,
        )
    )
    print(model_info.identifier(include_alias=True))
    return model_info.identifier(include_alias=True)

def upload_extra_files(identifier):

    features = ["mt1_bearing1", "mt1_bearing2", "mt1_bearing3", "mt1_bearing4"]
    features_json = json.dumps(features).encode("utf-8")

    wizata_dsapi.api().upload_file(
        identifier=identifier,
        path="features.json",
        content=features_json
    )

def download_model(identifier):
    model_info = wizata_dsapi.api().download_model(
        identifier=identifier
    )
    print(type(model_info.trained_model))

def download_model_and_extra_files(identifier):
    model_info = wizata_dsapi.api().download_model(
        identifier=identifier
    )
    for file in model_info.files:
        if file.path == "features.json":
            content = wizata_dsapi.api().download_file(model=model_info, file=file)
            features = json.loads(content)
            print(features)


if __name__ == "__main__":

    model = train_model()
    save_locally_pickle(model)
    upload_model_from_pickle()
    upload_model_from_pickle_as_bytes()
    model_identifier = upload_model(model)
    upload_extra_files(model_identifier)

    download_model(model_identifier)
    download_model_and_extra_files(model_identifier)


