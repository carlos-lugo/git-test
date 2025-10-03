# **fossy.link 開発のためのセットアップガイド**

このドキュメントは、ローカル開発環境で `fossy-api` (バックエンド) と `client` (フロントエンド) をセットアップし、デプロイするための手順を概説します。

-----

## **パート1: 前提条件と環境設定** ⚙️

このセクションでは、一度だけ必要となるツールのインストールとAWSの設定について説明します。

### **1. 開発ツールのインストール**

ターミナルを開き、各ツールがインストールされているか確認してください。インストールされていない場合は、提供されたコマンドを使用してください。

  * **Node.js (v20以上)**:

    ```bash
    # バージョンを確認
    node -v
    # 必要であればインストール
    brew install node
    ```

  * **Yarn**:

    ```bash
    # バージョンを確認
    yarn -v
    # 必要であればインストール
    brew install yarn
    ```

  * **AWS CLI**:

    ```bash
    # バージョンを確認
    aws --version
    # 必要であればインストール
    brew install awscli
    ```

  * **Serverless Framework**:

    ```bash
    # npm経由でグローバルにインストール
    npm install -g serverless
    # バージョンを確認
    sls --version
    ```

### **2. AWS IAMユーザーの作成**

リソースをデプロイするには、プログラムによるアクセスが可能なIAMユーザーが必要です。

1.  AWSマネジメントコンソールで**IAM**サービスに移動します。
2.  新しいユーザーを作成します (例: `fossy-dev-user`)。**アクセスキー - プログラムによるアクセス**を選択します。
3.  開発目的で**AdministratorAccess**ポリシーをアタッチします。**警告**: この設定を本番環境で使用しないでください。
4.  ユーザーを作成し、アクセスキーIDとシークレットアクセスキーを含む **.csvファイルをダウンロード**します。

### **3. AWS CLIプロファイルの設定**

このプロジェクト用に名前付きプロファイルを設定します。ここでは例として `fossy-dev` を使用しますが、好きな名前を選ぶことができます。

1.  ターミナルで `configure` コマンドを実行します:
    ```bash
    aws configure --profile fossy-dev
    ```
2.  ダウンロードした `.csv` ファイルから以下の詳細情報を入力します:
      * **AWS Access Key ID**: `YOUR_ACCESS_KEY`
      * **AWS Secret Access Key**: `YOUR_SECRET_KEY`
      * **Default region name**: `ap-northeast-1`
      * **Default output format**: `json`
3.  プロファイルが正常に作成されたことを確認します:
    ```bash
    # 設定されている全プロファイルをリスト表示
    aws configure list-profiles

    # 新しいプロファイルのID情報を確認
    aws sts get-caller-identity --profile fossy-dev
    ```

-----

## **パート2: バックエンドのデプロイ (fossy-api)** ☁️

このセクションでは、サーバーレスバックエンドの設定とデプロイ方法について詳述します。

### **1. プロジェクト依存関係のインストール**

APIディレクトリに移動し、その依存関係をインストールします。

```bash
cd path/to/your/project/src/fossy-api
yarn install
```

### **2. `serverless.yml` の設定**

`serverless.yml` ファイル内のいくつかの値を、あなたのAWS環境に合わせて変更する必要があります。

  * **10行目 (AWS Profile)**: プロファイルを先ほど設定したものに変更します。

      * **旧**: `profile: uniss`
      * **新**: `profile: fossy-dev`

  * **34行目 & 100行目 (S3 Bucket Name)**: S3バケット名はグローバルでユニークでなければなりません。バケット名にユニークな接尾辞（プロファイル名など）を追加します。

      * **旧**: `{Fn::Sub: "fossy-${self:provider.stage}"}`
      * **新**: `{Fn::Sub: "fossy-fossy-dev-${self:provider.stage}"}`

  * **40行目 (S3 Permissions)**: オブジェクトを公開できるようにするため、`PutObjectAcl` を追加します。

      * **追加**: `Action` リストの下に `"s3:PutObjectAcl"` を追加します。

  * **57行目 & 135行目 (SES Identity ARN)**: ARNをあなた自身の検証済みSESアイデンティティのものに置き換えます。

    1.  まず、[AWS SESコンソール](https://ap-northeast-1.console.aws.amazon.com/ses/home?region=ap-northeast-1#/identities)でEメールアイデンティティを検証します。
    2.  以下のプレースホルダーを、あなたの12桁のAWSアカウントIDと検証済みEメールに置き換えます。

    <!-- end list -->

      * **旧**: `"arn:aws:ses:us-east-1:577963771433:identity/noreply@fossy.link"`
          * **新**: `"arn:aws:ses:ap-northeast-1:YOUR_ACCOUNT_ID:identity/your-verified-email@example.com"`

  * **94行目 (DynamoDB Throughput)**: 開発中のコストを最小限に抑えるため、読み込みと書き込みのキャパシティを両方とも `1` に設定します。

      * **変更**: `ReadCapacityUnits: 1` と `WriteCapacityUnits: 1`

  * **201行目 (Custom Domain)**: 初回デプロイ時にデプロイが失敗するのを避けるため、`fossyApiDomainName` と `fossyApiBasePathMapping` リソースをコメントアウトします。

### **3. S3アップロードロジックの変更**

アップロードされたファイルが公開アクセス可能であることを保証するため、少しコードを変更します。

  * **ファイル**: `/src/fossy-api/commons/s3.js`
  * **アクション**: `s3.upload` のパラメータオブジェクトに `ACL: 'public-read'` プロパティを追加します。

### **4. APIのデプロイ**

`fossy-api` ディレクトリ内からデプロイコマンドを実行します。

```bash
sls deploy --verbose
```

### **5. S3バケットのパブリックアクセス設定**

デプロイ後、新しく作成されたS3バケットが公開画像を配信できるように手動で設定する必要があります。

1.  AWSコンソールで**S3**サービスに移動します。
2.  新しく作成されたバケット（例: `fossy-fossy-dev-stg`）を見つけます。
3.  **アクセス許可**タブに移動します。
4.  **オブジェクト所有者**の下にある「編集」を選択し、「**ACL有効**」を選びます。
5.  **パブリックアクセスをすべてブロック**の下にある「編集」を選択し、「**パブリックアクセスをすべてブロック**」の**チェックを外します**。変更を保存します。

-----

## **パート3: フロントエンドのセットアップと起動 (client)** 🖥️

デプロイしたバックエンドに接続するようにクライアントアプリケーションを設定します。

### **1. APIとCognito詳細の設定**

バックエンドのデプロイからの出力で、以下の2つのファイルを更新します。

  * **ファイル**: `/src/client/src/js/config.js`

      * **アクション**: プレースホルダーの `userPoolId` と `userPoolClientId` の値を、Cognitoユーザープールのデプロイ出力から得られる実際のIDに置き換えます。

  * **ファイル**: `/src/client/src/js/scripts.js`

      * **`API_URL`**: プレースホルダーをAPI Gatewayのデプロイステージの**呼び出しURL**に置き換えます。
      * **`STATIC_URL`**: これをS3バケットのURLに設定します（例: `https://fossy-fossy-dev-stg.s3.ap-northeast-1.amazonaws.com`）。

### **2. フロントエンドの起動**

1.  クライアントディレクトリに移動し、依存関係をインストールします。
    ```bash
    cd path/to/your/project/src/client
    yarn
    ```
2.  ローカル開発サーバーを起動します。
    ```bash
    yarn start
    ```

-----

## **パート4: デプロイ後の作業とテスト** ✅

### **1. 管理者ユーザーの作成**

1.  ローカルのフロントエンドから新しいユーザーを登録します。
2.  ログインし、個人情報を入力してからログアウトします。
3.  AWSコンソールで**Cognitoユーザープール**に移動します。
4.  該当ユーザーを見つけ、そのロール属性を**MANAGER**に変更します。

### **2. 関数のテスト (ローカル & AWS)**

迅速なテストのためにLambda関数をローカルで呼び出すことも、実際のAWSリソースでテストするためにAWS上で呼び出すこともできます。

```bash
# 関数名を環境変数として設定
export FUNCTION_NAME="myFunction"

# 関数をローカルで実行 (AWS環境をシミュレート)
sls invoke local --function "${FUNCTION_NAME}" --path "events/${FUNCTION_NAME}.json"

# 関数をAWS上で実行
sls invoke --function "${FUNCTION_NAME}" --path "events/${FUNCTION_NAME}.json"

# デプロイされた関数のライブログを表示
sls logs --function "${FUNCTION_NAME}" --tail
```

-----

## **パート5: 高度なトピックとクリーンアップ** 📚

### **1. DynamoDBコストの管理**

`serverless.yml` ファイルは、高額なコストを防ぐためにDynamoDBのキャパシティを `1` に設定しています。これはAWSコンソールで手動で確認・編集することもできます:

  * **DynamoDB** -\> **テーブル** -\> 対象のテーブルを選択 -\> **アクション** -\> **キャパシティーを編集**に移動します。\*

### **2. トラブルシューティングとデバッグ**

  * **S3のPDFパーミッション**: サービスによって作成されたPDFは、画像のように自動的にパブリック読み取り権限を取得しない場合があることに注意してください。これは既知の問題です。
  * **API Gatewayのログ**: APIの問題をデバッグする必要がある場合、CloudWatchロギングの設定が不可欠です。
      * [公式ガイド: REST API の CloudWatch ログ記録をセットアップする](https://docs.aws.amazon.com/ja_jp/apigateway/latest/developerguide/set-up-logging.html)
      * [Troubleshooting Role ARN Errors](https://coady.tech/aws-cloudwatch-logs-arn/)
      * ログを表示するには、**CloudWatch** -\> **ロググループ**に移動します。`/aws/api-gateway/your-api-name` という名前のグループを探します。

### **3. リソースのクリーンアップ**

このスタック用にデプロイされた**すべての**AWSリソースを削除し、課金を停止するには、`fossy-api` ディレクトリから `remove` コマンドを実行します。

```bash
sls remove
```