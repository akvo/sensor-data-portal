FROM node:5.11

RUN git clone https://github.com/pvgennip/spul-mqtt-server.git data
RUN curl https://raw.githubusercontent.com/pvgennip/spul-mqtt-server/master/lib/connector.js > /data/lib/connector.js

RUN touch /tmp/output.log

WORKDIR /data

RUN npm install --production

EXPOSE 9007
EXPOSE 9008

CMD ["node", "lib/connector.js"]
