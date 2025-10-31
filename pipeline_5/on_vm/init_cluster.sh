set -e
set -x

apt-get update
apt-get install -y python3-pip
pip3 install --upgrade pip
pip3 install nltk==3.8.1 textblob==0.17.1 pandas==2.0.3

python3 << EOF
import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('vader_lexicon', quiet=True)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
EOF
