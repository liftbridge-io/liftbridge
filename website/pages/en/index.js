const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const ProjectTitle = props => (
      <div>
        <img src={props.img_src} alt="Liftbridge" id="logo" />
        <h1 className="index-tagline">{siteConfig.tagline}</h1>
      </div>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (
      <SplashContainer>
        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} img_src={`${baseUrl}img/liftbridge_full.png`} />
          <PromoSection>
          <a
            className="button index-cta-button"
            href={docUrl('quick-start.html')}>
            Get Started
          </a>
          <a
            className="button index-cta-button"
            href={docUrl('faq.html')}>
            FAQ
          </a>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl} = siteConfig;

    const Block = props => (
      <Container
        padding={['bottom', 'top']}
        id={props.id}
        className={props.className}
        background={props.background}>
        <GridBlock
          align={props.align}
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const UseCases = () => (
      <Block className="navy-background" align="left">
        {[
          {
            content:
              '<p class="use-case">Pub/sub messaging</p>' +
              '<p class="use-case">Event sourcing and CQRS</p>' +
              '<p class="use-case">Stream processing (e.g. processing clickstream events)</p>' +
              '<p class="use-case">Real-time data pipelines (e.g. log or metric aggregation to feed disparate backends)</p>' +
              '<p class="use-case">Replicated transaction commit logs (e.g. materialize views, populate caches, update indexes, or synchronize data)</p>',
            image: `${baseUrl}img/use_cases.svg`,
            imageAlign: 'right',
            title: 'Use Cases',
          },
        ]}
      </Block>
    );

    const Features = () => (
      <Block layout="threeColumn" align="center">
        {[
          {
            content: 'Extend <a href="https://nats.io/">NATS</a> with a Kafka-like durable pub/sub log API. ' +
                     'Use Liftbridge as a simpler and lighter alternative to ' +
                     'systems like Kafka and Pulsar or to add streaming semantics ' +
                     'to an existing NATS deployment.<br><br>',
            image: `${baseUrl}img/nats.png`,
            imageAlign: 'top',
            title: 'Pub/Sub Log API for NATS',
          },
          {
            content: 'Stream replication provides high availability and durability ' +
                     'of messages. Clustering and partitioning provides ' +
                     'horizontal scalability for streams and their consumers.<br><br>' ,
            image: `${baseUrl}img/scalability.png`,
            imageAlign: 'top',
            title: 'Fault-Tolerant and Scalable',
          },
          {
            content: 'No heavy or unwieldy dependencies like ZooKeeper or the JVM. ' +
                     'Liftbridge is a single static binary roughly 16MB in size. ' +
                     'It has a simple gRPC-based API which makes it quick to implement ' +
                     'client libraries.<br><br>',
            image: `${baseUrl}img/zen.png`,
            imageAlign: 'top',
            title: 'Supremely Simple',
          },
          {
            content: 'Create streams that match wildcard topics, such as ' +
                     'stock.nyse.* or stock.nasdaq.* in addition to topic literals ' +
                     'like stock.nasdaq.msft.<br><br>',
            image: `${baseUrl}img/asterisk.png`,
            imageAlign: 'top',
            title: 'Wildcard Subscriptions',
          },
          {
            content: 'Messages can have a key set on them for key-value semantics ' +
                     'and other arbitrary headers, making Liftbridge a great choice ' +
                     'for transaction write-ahead logs.<br><br>',
            image: `${baseUrl}img/key_value.png`,
            imageAlign: 'top',
            title: 'Key-Value and Header Support',
          },
          {
            content: 'In addition to retention policies based on time and data ' +
                     'volume, Liftbridge supports log compaction by key. This ' +
                     'means the server retains only the latest value for a given key.<br><br>',
            image: `${baseUrl}img/log_retention.png`,
            imageAlign: 'top',
            title: 'Log Retention and Compaction',
          },
        ]}
      </Block>
    );

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
        </div>
        <UseCases />
      </div>
    );
  }
}

module.exports = Index;
