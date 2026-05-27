namespace Kuestenlogik.Surgewave.AkkaStreams.Tests;

using Kuestenlogik.Surgewave.AkkaStreams.Consumer;
using Kuestenlogik.Surgewave.AkkaStreams.Subscriptions;
using Xunit;

/// <summary>
/// Tests for PlainSource consumer stage.
/// </summary>
public class PlainSourceSpec
{
    [Fact]
    public void Subscriptions_Topics_should_create_TopicSubscription()
    {
        var sub = Subscriptions.Topics("topic1", "topic2");
        var ts = Assert.IsType<TopicSubscription>(sub);
        Assert.Equal(2, ts.Topics.Count);
        Assert.Equal("topic1", ts.Topics[0]);
        Assert.Equal("topic2", ts.Topics[1]);
    }

    [Fact]
    public void Subscriptions_Assignment_should_create_AssignmentSubscription()
    {
        var sub = Subscriptions.Assignment(("topic1", 0), ("topic1", 1));
        var a = Assert.IsType<AssignmentSubscription>(sub);
        Assert.Equal(2, a.Assignments.Count);
    }

    [Fact]
    public void Subscriptions_TopicPattern_should_create_PatternSubscription()
    {
        var sub = Subscriptions.TopicPattern("akka-.*");
        var tp = Assert.IsType<TopicPatternSubscription>(sub);
        Assert.Equal("akka-.*", tp.Pattern);
    }
}
